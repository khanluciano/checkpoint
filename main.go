package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
)

// TrafficPayload represents the live broadcast message structure sent to clients
type TrafficPayload struct {
	Active int `json:"active"`
	Total  int `json:"total"`
}

var (
	// Upgrader configures CORS rules to accept connection streams safely
	upgrader = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}

	// State maps and lock matrices
	mu             sync.Mutex
	activeSockets  = make(map[*websocket.Conn]string) // WebSocket -> Unique Token Key
	uniqueVisitors = make(map[string]bool)             // Stores cumulative distinct historical visits
)

func handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("Upgrade handshake error: %v", err)
		return
	}

	// Capture identification parameters: fallback to incoming Remote Address IP if local storage token is missing
	uid := r.URL.Query().Get("uid")
	if uid == "" {
		uid = r.RemoteAddr
	}

	mu.Lock()
	activeSockets[conn] = uid
	uniqueVisitors[uid] = true
	log.Printf("Connection accepted from identifier token: %s", uid)
	mu.Unlock()

	// Immediately notify everyone about the updated metrics update
	broadcastTrafficMetrics()

	// Connection keeping loop listener block
	defer func() {
		mu.Lock()
		delete(activeSockets, conn)
		mu.Unlock()
		conn.Close()
		broadcastTrafficMetrics()
	}()

	for {
		// Keep listening to track if the client closes the browser or disconnects
		_, _, err := conn.ReadMessage()
		if err != nil {
			break
		}
	}
}

func broadcastTrafficMetrics() {
	mu.Lock()
	defer mu.Unlock()

	totalVisitorsCount := len(uniqueVisitors)

	// Filter unique identifiers among currently active socket pipelines
	activeUniqueMap := make(map[string]bool)
	for _, uid := range activeSockets {
		activeUniqueMap[uid] = true
	}
	activeCount := len(activeUniqueMap)

	payload := TrafficPayload{
		Active: activeCount,
		Total:  totalVisitorsCount,
	}

	msg, err := json.Marshal(payload)
	if err != nil {
		log.Printf("JSON marshaling failed: %v", err)
		return
	}

	// Push state packet updates out concurrently to all live pipelines
	for clientConn := range activeSockets {
		err := clientConn.WriteMessage(websocket.TextMessage, msg)
		if err != nil {
			log.Printf("Broadcast send tracking failure. Cleared node pipeline channel: %v", err)
			clientConn.Close()
			delete(activeSockets, clientConn)
		}
	}
}

func main() {
	// Serve static workspace files directly out of the execution root path directory
	fs := http.FileServer(http.Dir("./"))
	http.Handle("/", fs)

	// Route handler for analytics web socket channels
	http.HandleFunc("/ws", handleWebSocket)

	port := ":8080"
	fmt.Printf("Luciano platform server successfully initialized. Listening securely over http://localhost%s\n", port)
	if err := http.ListenAndServe(port, nil); err != nil {
		log.Fatalf("Server startup exception: %v", err)
	}
}
