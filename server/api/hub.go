package api

// WebSocket event envelope (cross-component contract with the frontend).
//
// Every message broadcast to WebSocket clients is a JSON object with this shape:
//
//	{
//	  "type": "cb_state_change" | "rate_limit_result" | "sim_stats" | "connected",
//	  "name": "<limiter algorithm or circuit-breaker name>",  // topic key; "" for global
//	  "data": { ... },                                        // type-specific payload
//	  "ts":   <unix milliseconds>
//	}
//
// Event types:
//   - "connected":         sent once on connect; data carries {"filter": "<topic>"}.
//   - "rate_limit_result": emitted after an allow decision; data is rateLimitResponse.
//   - "cb_state_change":   emitted on a circuit-breaker state transition or execute;
//                          data is the circuit-breaker snapshotJSON.
//   - "sim_stats":         emitted with simulation results; data is simulation.Result.
//
// The "name" field is used for topic filtering: a client subscribed to a specific
// algorithm/CB only receives events whose "name" matches its filter (an empty
// client filter — the /ws/v1/events endpoint — receives everything).
//
// This envelope is STABLE. Do not change field names/semantics without updating
// the frontend consumer in lock-step.

import (
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// WS write/ping tuning (M-22). pongWait must exceed pingPeriod so a live client
// always answers a ping before its read deadline elapses.
const (
	writeWait  = 10 * time.Second
	pongWait   = 60 * time.Second
	pingPeriod = (pongWait * 9) / 10 // 54s
)

// Event is the JSON envelope broadcast to WebSocket clients. See the package
// comment at the top of this file for the contract.
type Event struct {
	Type string `json:"type"`
	Name string `json:"name,omitempty"`
	Data any    `json:"data,omitempty"`
	TS   int64  `json:"ts"`
}

// newEventBytes marshals an Event with the current timestamp. On marshal error
// it returns nil (callers skip broadcasting).
func newEventBytes(eventType, name string, data any) []byte {
	b, err := json.Marshal(Event{
		Type: eventType,
		Name: name,
		Data: data,
		TS:   time.Now().UnixMilli(),
	})
	if err != nil {
		return nil
	}
	return b
}

// Client represents a connected WebSocket client.
type Client struct {
	hub       *Hub
	conn      *websocket.Conn
	send      chan []byte
	filter    string // optional topic filter (e.g. algorithm name, cb name)
	closeOnce sync.Once
}

// closeSend closes the client's send channel exactly once, guarding against the
// double-close panic when both the hub shutdown path and the unregister path run.
func (c *Client) closeSend() {
	c.closeOnce.Do(func() { close(c.send) })
}

// Hub manages the set of active WebSocket clients and broadcasts messages.
type Hub struct {
	clients    map[*Client]bool
	broadcast  chan []byte
	register   chan *Client
	unregister chan *Client
	mu         sync.RWMutex
	logger     *slog.Logger
	done       chan struct{}
}

// newHub creates a new Hub.
func newHub(logger *slog.Logger) *Hub {
	return &Hub{
		clients:    make(map[*Client]bool),
		broadcast:  make(chan []byte, 256),
		register:   make(chan *Client, 16),
		unregister: make(chan *Client, 16),
		logger:     logger,
		done:       make(chan struct{}),
	}
}

// Run starts the hub's event loop. Call in a goroutine.
func (h *Hub) Run() {
	for {
		select {
		case <-h.done:
			h.mu.Lock()
			for c := range h.clients {
				// H-17: close each send channel so the client's writePump
				// (for msg := range c.send) unblocks and its goroutine exits.
				// Guarded against double-close via closeOnce.
				c.closeSend()
				if c.conn != nil {
					c.conn.Close()
				}
				delete(h.clients, c)
			}
			h.mu.Unlock()
			return

		case c := <-h.register:
			h.mu.Lock()
			h.clients[c] = true
			// Send the welcome from inside the hub goroutine — the only place
			// that ever closes c.send (done/unregister cases) — so it can never
			// race a concurrent close and panic (F-3). Non-blocking: a fresh
			// client's 64-slot buffer has room.
			welcome, _ := json.Marshal(Event{
				Type: "connected",
				Name: c.filter,
				Data: map[string]string{"filter": c.filter},
				TS:   time.Now().UnixMilli(),
			})
			select {
			case c.send <- welcome:
			default:
			}
			h.mu.Unlock()

		case c := <-h.unregister:
			h.mu.Lock()
			if _, ok := h.clients[c]; ok {
				delete(h.clients, c)
				c.closeSend()
			}
			h.mu.Unlock()

		case msg := <-h.broadcast:
			h.mu.RLock()
			for c := range h.clients {
				// Topic filtering: a client with a non-empty filter only
				// receives events whose "name" matches. Empty filter = all.
				if !clientWantsMessage(c, msg) {
					continue
				}
				select {
				case c.send <- msg:
				default:
					// Slow client — drop message
				}
			}
			h.mu.RUnlock()
		}
	}
}

// clientWantsMessage reports whether the message should be delivered to c based
// on its topic filter. Messages that don't parse as an Event, or have no name,
// are delivered to everyone (e.g. the "connected" welcome).
func clientWantsMessage(c *Client, msg []byte) bool {
	if c.filter == "" {
		return true
	}
	var ev Event
	if err := json.Unmarshal(msg, &ev); err != nil || ev.Name == "" {
		return true
	}
	return ev.Name == c.filter
}

// Broadcast sends a raw pre-marshalled message to all connected clients.
func (h *Hub) Broadcast(msg []byte) {
	if msg == nil {
		return
	}
	select {
	case h.broadcast <- msg:
	default:
		h.logger.Warn("hub broadcast channel full, dropping message")
	}
}

// BroadcastEvent marshals and broadcasts a typed event using the standard
// envelope. It is the preferred entry point for handlers (H-19).
func (h *Hub) BroadcastEvent(eventType, name string, data any) {
	h.Broadcast(newEventBytes(eventType, name, data))
}

// Stop gracefully shuts down the hub.
func (h *Hub) Stop() {
	close(h.done)
}

// writePump pumps messages from the hub to the WebSocket connection and drives
// the keepalive ping ticker. A write deadline is set before every write so a
// stuck peer cannot zombie this goroutine (M-22).
func (c *Client) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()

	for {
		select {
		case msg, ok := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait)) //nolint:errcheck
			if !ok {
				// Hub closed the channel — send a close frame and exit.
				c.conn.WriteMessage(websocket.CloseMessage, []byte{}) //nolint:errcheck
				return
			}
			if err := c.conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				return
			}
		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait)) //nolint:errcheck
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}
