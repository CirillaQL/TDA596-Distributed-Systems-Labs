package server

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/CirillaQL/TDA596-Distributed-Systems-Labs/lab1/utils"
)

var AllowedExtensions = []string{".html", ".txt", ".gif", ".jpeg", ".jpg", ".css"}

type Server struct {
	port           uint32
	maxConnections uint32
	connLimiter    chan struct{}
	broker         *Broker
}

// New creates a file server for the supplied TCP port.
func NewServer(port uint16) *Server {
	return &Server{port: uint32(port), maxConnections: 10, broker: NewBroker()}
}

// fileExtensionFilter middleware
func (s *Server) fileExtensionFilter(conn net.Conn, r *http.Request) bool {
	ext := strings.ToLower(filepath.Ext(r.URL.Path))
	if !slices.Contains(AllowedExtensions, ext) {
		utils.Send400BadRequest(conn, "Forbidden file extension")
		return false
	}
	return true
}

func (s *Server) Start() error {
	if s.port == 0 {
		return fmt.Errorf("port must be between 1 and 65535")
	}

	if s.maxConnections == 0 {
		log.Printf("Max connections not set, defaulting to 10")
		s.maxConnections = 10
	}

	s.connLimiter = make(chan struct{}, s.maxConnections)

	addr := fmt.Sprintf(":%d", s.port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to start server: %w", err)
	}
	defer listener.Close()

	log.Printf("Server listening on %s (max connections: %d)", addr, s.maxConnections)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Failed to accept connection: %v", err)
			continue
		}

		s.connLimiter <- struct{}{}
		go s.handleConnection(conn)
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	defer func() {
		<-s.connLimiter
		conn.Close()
	}()

	log.Printf("Accepted connection from %s", conn.RemoteAddr())

	reader := bufio.NewReader(conn)
	req, err := http.ReadRequest(reader)

	if err != nil {
		log.Printf("Failed to parse HTTP request: %v", err)
		utils.Send400BadRequest(conn, "Malformed HTTP request")
		return
	}

	switch req.URL.Path {
	case "/subscribe", "/unsubscribe", "/publish", "/events":
		s.handleBroker(conn, req)
		return
	}

	if req.Method != http.MethodGet && req.Method != http.MethodPost {
		log.Printf("Method %s not implemented for %s", req.Method, req.URL.Path)
		utils.Send501NotImplemented(conn, "Method Not Implemented")
		return
	}

	if !s.fileExtensionFilter(conn, req) {
		log.Printf("Request for %s rejected due to file extension", req.URL.Path)
		return
	}

	switch req.Method {
	case http.MethodGet:
		s.handleGet(conn, req)
	case http.MethodPost:
		s.handlePost(conn, req)
	}
}

func (s *Server) handleBroker(conn net.Conn, req *http.Request) {
	defer req.Body.Close()
	wantMethod := http.MethodPost
	if req.URL.Path == "/events" {
		wantMethod = http.MethodGet
	}
	if req.Method != wantMethod {
		utils.Send501NotImplemented(conn, "Method Not Implemented")
		return
	}

	query := req.URL.Query()
	topic := query.Get("topic")
	if topic == "" {
		utils.Send400BadRequest(conn, "Missing topic")
		return
	}

	switch req.URL.Path {
	case "/subscribe":
		id, _ := s.broker.Subscribe(topic)
		utils.Send200OK(conn, fmt.Sprintf(`{"id":%d}`, id), "application/json")
	case "/publish":
		data, err := io.ReadAll(req.Body)
		if err != nil {
			utils.Send400BadRequest(conn, "Failed to read message")
			return
		}
		s.broker.Publish(topic, data)
		utils.Send200OK(conn, "Message published", "text/plain")
	case "/unsubscribe", "/events":
		id, err := strconv.Atoi(query.Get("id"))
		if err != nil || id < 0 {
			utils.Send400BadRequest(conn, "Invalid subscriber id")
			return
		}
		if req.URL.Path == "/unsubscribe" {
			if !s.broker.Unsubscribe(topic, id) {
				utils.Send400BadRequest(conn, "Invalid subscriber id")
				return
			}
			utils.Send200OK(conn, "Unsubscribed", "text/plain")
			return
		}

		events := s.broker.Event(topic, id)
		if events == nil {
			utils.Send400BadRequest(conn, "Invalid subscriber id")
			return
		}
		msg, status := s.waitForEvent(events, 30*time.Second)
		switch status {
		case http.StatusNoContent:
			utils.SendHTTPResponse(conn, 204, "No Content", "", "text/plain")
		case http.StatusBadRequest:
			utils.Send400BadRequest(conn, "Subscription cancelled")
		default:
			body, err := json.Marshal(msg)
			if err != nil {
				utils.Send400BadRequest(conn, "Failed to encode message")
				return
			}
			utils.Send200OKBytes(conn, body, "application/json")
		}
	}
}

func (s *Server) waitForEvent(events <-chan Message, timeout time.Duration) (Message, int) {
	<-s.connLimiter
	defer func() { s.connLimiter <- struct{}{} }()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case msg, ok := <-events:
		if !ok {
			return Message{}, http.StatusBadRequest
		}
		return msg, http.StatusOK
	case <-timer.C:
		return Message{}, http.StatusNoContent
	}
}

func (s *Server) handleGet(conn net.Conn, req *http.Request) {
	cleanPath := filepath.Clean(req.URL.Path)
	filePath := "." + cleanPath

	data, err := os.ReadFile(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			utils.Send404NotFound(conn, "File Not Found")
			return
		}
		utils.Send400BadRequest(conn, "Failed to read file")
		return
	}

	contentType := "text/plain"
	switch strings.ToLower(filepath.Ext(filePath)) {
	case ".html":
		contentType = "text/html"
	case ".css":
		contentType = "text/css"
	case ".gif":
		contentType = "image/gif"
	case ".jpeg", ".jpg":
		contentType = "image/jpeg"
	}

	utils.Send200OKBytes(conn, data, contentType)
}

func (s *Server) handlePost(conn net.Conn, req *http.Request) {
	defer req.Body.Close()

	cleanPath := filepath.Clean(req.URL.Path)
	targetPath := "." + cleanPath

	dir := filepath.Dir(targetPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		utils.Send400BadRequest(conn, "Failed to create dir")
		return
	}

	tmpFile, err := os.CreateTemp(dir, "."+filepath.Base(targetPath)+".tmp.*")
	if err != nil {
		utils.Send400BadRequest(conn, "Failed to create temp file")
		return
	}
	tmpFileName := tmpFile.Name()

	defer func() {
		tmpFile.Close()
		os.Remove(tmpFileName)
	}()

	if _, err := io.Copy(tmpFile, req.Body); err != nil {
		utils.Send400BadRequest(conn, "Failed to write body")
		return
	}

	if err := tmpFile.Sync(); err != nil {
		utils.Send400BadRequest(conn, "Failed to sync file")
		return
	}
	if err := tmpFile.Close(); err != nil {
		utils.Send400BadRequest(conn, "Failed to close file")
		return
	}

	if err := os.Rename(tmpFileName, targetPath); err != nil {
		utils.Send400BadRequest(conn, "Failed to save file")
		return
	}

	utils.Send200OK(conn, "File saved successfully", "text/plain")
}
