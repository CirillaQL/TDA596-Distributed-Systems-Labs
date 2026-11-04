package utils

import (
	"fmt"
	"net"
)

// HTTP Response 助手函数：统一发送格式化的 HTTP 响应
func SendHTTPResponse(conn net.Conn, statusCode int, statusText string, body string, contentType string) {
	if contentType == "" {
		contentType = "text/plain"
	}

	bodyBytes := []byte(body)
	contentLength := len(bodyBytes)

	header := fmt.Sprintf(
		"HTTP/1.1 %d %s\r\n"+
			"Content-Type: %s\r\n"+
			"Content-Length: %d\r\n"+
			"Connection: close\r\n"+
			"\r\n",
		statusCode, statusText, contentType, contentLength,
	)

	conn.Write([]byte(header))
	if contentLength > 0 {
		conn.Write(bodyBytes)
	}
}

// 200 OK with text body
func Send200OK(conn net.Conn, body string, contentType string) {
	SendHTTPResponse(conn, 200, "OK", body, contentType)
}

// 200 OK with binary body
func Send200OKBytes(conn net.Conn, body []byte, contentType string) {
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	header := fmt.Sprintf(
		"HTTP/1.1 200 OK\r\n"+
			"Content-Type: %s\r\n"+
			"Content-Length: %d\r\n"+
			"Connection: close\r\n"+
			"\r\n",
		contentType, len(body),
	)
	conn.Write([]byte(header))
	conn.Write(body)
}

// 400 Bad Request
func Send400BadRequest(conn net.Conn, msg string) {
	if msg == "" {
		msg = "Bad Request"
	}
	SendHTTPResponse(conn, 400, "Bad Request", msg, "text/plain")
}

// 404 Not Found
func Send404NotFound(conn net.Conn, msg string) {
	if msg == "" {
		msg = "File Not Found"
	}
	SendHTTPResponse(conn, 404, "Not Found", msg, "text/plain")
}

// 500 Internal Server Error
func Send500InternalError(conn net.Conn, msg string) {
	if msg == "" {
		msg = "Internal Server Error"
	}
	SendHTTPResponse(conn, 500, "Internal Server Error", msg, "text/plain")
}

// 501 Not Implemented
func Send501NotImplemented(conn net.Conn, msg string) {
	if msg == "" {
		msg = "Method Not Implemented"
	}
	SendHTTPResponse(conn, 501, "Not Implemented", msg, "text/plain")
}

// 503 Service Unavailable
func Send503ServiceUnavailable(conn net.Conn, msg string) {
	if msg == "" {
		msg = "Service Unavailable"
	}
	SendHTTPResponse(conn, 503, "Service Unavailable", msg, "text/plain")
}
