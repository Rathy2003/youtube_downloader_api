package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"log"
	"mime"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"time"
)

// ---------- config (env) ----------
var (
	port          = getenv("PORT", "8080")
	allowedOrigin = getenv("ALLOWED_ORIGIN", "*") // set to your site, e.g. https://example.com
	downloadDir   = getenv("DOWNLOAD_DIR", "./downloads")
	maxDurationS  = getenv("MAX_DURATION_SECONDS", "3600")
	ytdlpPath     = getenv("YTDLP_PATH", "./lib/yt-dlp")
	ffmpegPath    = getenv("FFMPEG_PATH", "./lib/ffmpeg")
)

const (
	maxConcurrent = 3                // parallel conversions
	jobTTL        = 30 * time.Minute // files/jobs are deleted after this
	jobTimeout    = 10 * time.Minute // hard limit per conversion
)

var (
	videoIDRe = regexp.MustCompile(`^[a-zA-Z0-9_-]{11}$`)
	jobIDRe   = regexp.MustCompile(`^[a-f0-9]{32}$`)
	unsafeRe  = regexp.MustCompile(`[\\/:*?"<>|\x00-\x1f]`)
)

// ---------- job store ----------
type job struct {
	Status  string `json:"status"` // checking | extracting | converting | completed
	Title   string `json:"title,omitempty"`
	Error   string `json:"error,omitempty"`
	created time.Time
}

var (
	mu   sync.RWMutex
	jobs = map[string]*job{}
	sem  = make(chan struct{}, maxConcurrent)
)

func setJob(id string, fn func(j *job)) {
	mu.Lock()
	defer mu.Unlock()
	if j, ok := jobs[id]; ok {
		fn(j)
	}
}

func getJob(id string) (job, bool) {
	mu.RLock()
	defer mu.RUnlock()
	j, ok := jobs[id]
	if !ok {
		return job{}, false
	}
	return *j, true
}

func resolve(p string) string {
	if runtime.GOOS == "windows" && !strings.HasSuffix(p, ".exe") {
		p += ".exe"
	}
	abs, err := filepath.Abs(p)
	if err != nil {
		log.Fatal(err)
	}
	return abs
}

// ---------- main ----------
func main() {
	ytdlpPath = resolve(ytdlpPath)
	ffmpegPath = resolve(ffmpegPath)
	for _, bin := range []string{ytdlpPath, ffmpegPath} {
		if _, err := os.Stat(bin); err != nil {
			log.Fatalf("binary not found: %s", bin)
		}
	}
	if err := os.MkdirAll(downloadDir, 0o755); err != nil {
		log.Fatal(err)
	}
	go cleanupLoop()

	mux := http.NewServeMux()
	mux.HandleFunc("/api/convert", handleConvert)
	mux.HandleFunc("/api/progress", handleProgress)
	mux.HandleFunc("/api/download", handleDownload)

	srv := &http.Server{
		Addr:              ":" + port,
		Handler:           cors(mux),
		ReadHeaderTimeout: 10 * time.Second,
	}
	log.Println("listening on :" + port)
	log.Fatal(srv.ListenAndServe())
}

// ---------- middleware ----------
func cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", allowedOrigin)
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// ---------- handlers ----------

// POST /api/convert  {"videoId":"dQw4w9WgXcQ","format":"mp3"} -> {"id":"..."}
func handleConvert(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1024)

	var req struct {
		VideoID string `json:"videoId"`
		Format  string `json:"format"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid body"})
		return
	}
	if !videoIDRe.MatchString(req.VideoID) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid video id"})
		return
	}
	if req.Format != "mp3" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "only mp3 is supported"})
		return
	}

	select {
	case sem <- struct{}{}:
	default:
		writeJSON(w, http.StatusTooManyRequests, map[string]string{"error": "server busy, try again shortly"})
		return
	}

	id := newID()
	mu.Lock()
	jobs[id] = &job{Status: "checking", created: time.Now()}
	mu.Unlock()

	go func() {
		defer func() { <-sem }()
		runJob(id, req.VideoID)
	}()

	writeJSON(w, http.StatusOK, map[string]string{"id": id})
}

// GET /api/progress?id=ID
func handleProgress(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	if !jobIDRe.MatchString(id) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid id"})
		return
	}
	j, ok := getJob(id)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "job not found"})
		return
	}
	writeJSON(w, http.StatusOK, j)
}

// GET /api/download?id=ID
func handleDownload(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	if !jobIDRe.MatchString(id) {
		http.Error(w, "invalid id", http.StatusBadRequest)
		return
	}
	j, ok := getJob(id)
	if !ok || j.Status != "completed" {
		http.Error(w, "not ready", http.StatusNotFound)
		return
	}
	path := filepath.Join(downloadDir, id+".mp3")
	f, err := os.Open(path)
	if err != nil {
		http.Error(w, "file missing", http.StatusNotFound)
		return
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		http.Error(w, "file error", http.StatusInternalServerError)
		return
	}

	name := strings.TrimSpace(unsafeRe.ReplaceAllString(j.Title, ""))
	if name == "" {
		name = "audio"
	}
	w.Header().Set("Content-Type", "audio/mpeg")
	w.Header().Set("Content-Disposition", mime.FormatMediaType("attachment", map[string]string{"filename": name + ".mp3"}))
	http.ServeContent(w, r, "", st.ModTime(), f)
}

// ---------- conversion ----------
func runJob(id, videoID string) {
	ctx, cancel := context.WithTimeout(context.Background(), jobTimeout)
	defer cancel()

	url := "https://www.youtube.com/watch?v=" + videoID // videoID is regex-validated; no shell is used

	fail := func(msg string, err error) {
		log.Printf("job %s: %s: %v", id, msg, err)
		setJob(id, func(j *job) { j.Error = msg })
	}

	// 1) checking: fetch title + enforce max duration
	out, err := exec.CommandContext(ctx, ytdlpPath,
		"--no-playlist", "--skip-download",
		"--match-filter", "duration<="+maxDurationS,
		"--print", "%(title)s",
		url,
	).Output()
	title := strings.TrimSpace(string(out))
	if err != nil || title == "" {
		fail("video unavailable or too long", err)
		return
	}
	setJob(id, func(j *job) { j.Title = title; j.Status = "extracting" })

	// 2) extract + convert, tracking progress from yt-dlp's output
	outTmpl := filepath.Join(downloadDir, id+".%(ext)s")
	cmd := exec.CommandContext(ctx, ytdlpPath,
		"--no-playlist", "--newline",
		"--ffmpeg-location", ffmpegPath,
		"-x", "--audio-format", "mp3", "--audio-quality", "0",
		"-o", outTmpl,
		url,
	)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		fail("internal error", err)
		return
	}
	if err := cmd.Start(); err != nil {
		fail("internal error", err)
		return
	}
	sc := bufio.NewScanner(stdout)
	for sc.Scan() {
		if strings.HasPrefix(sc.Text(), "[ExtractAudio]") {
			setJob(id, func(j *job) { j.Status = "converting" })
		}
	}
	if err := cmd.Wait(); err != nil {
		fail("conversion failed", err)
		return
	}
	if _, err := os.Stat(filepath.Join(downloadDir, id+".mp3")); err != nil {
		fail("conversion failed", err)
		return
	}
	setJob(id, func(j *job) { j.Status = "completed" })
}

// ---------- housekeeping ----------
func cleanupLoop() {
	t := time.NewTicker(5 * time.Minute)
	for range t.C {
		cutoff := time.Now().Add(-jobTTL)
		mu.Lock()
		for id, j := range jobs {
			if j.created.Before(cutoff) {
				delete(jobs, id)
				matches, _ := filepath.Glob(filepath.Join(downloadDir, id+".*"))
				for _, m := range matches {
					_ = os.Remove(m)
				}
			}
		}
		mu.Unlock()
	}
}

func newID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
