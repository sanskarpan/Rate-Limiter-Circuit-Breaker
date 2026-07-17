package api

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/sanskarpan/Rate-Limiter-Circuit-Breaker/circuitbreaker"
)

// maxWSMessageBytes is the maximum WebSocket message size (512 KiB).
const maxWSMessageBytes = 512 * 1024

// WSHandler handles WebSocket upgrade requests.
type WSHandler struct {
	hub           *Hub
	logger        *slog.Logger
	upgrader      websocket.Upgrader
	knownLimiters map[string]bool
	registry      *circuitbreaker.Registry
}

// newWSHandler creates a new WSHandler with origin-aware upgrader.
//
// M-20: the "*" wildcard is intentionally NOT honoured for WebSocket upgrades.
// Cross-site WebSocket hijacking (CSWSH) bypasses CORS, so origins are validated
// against an explicit allow-list; a "*" entry never opens the gate.
func newWSHandler(
	hub *Hub,
	logger *slog.Logger,
	corsOrigins []string,
	knownLimiters map[string]bool,
	registry *circuitbreaker.Registry,
) *WSHandler {
	allowedOrigins := make(map[string]bool, len(corsOrigins))
	for _, o := range corsOrigins {
		trimmed := strings.TrimSpace(o)
		if trimmed == "" || trimmed == "*" {
			// Never add the wildcard to the WS allow-list (M-20).
			continue
		}
		allowedOrigins[trimmed] = true
	}

	return &WSHandler{
		hub:           hub,
		logger:        logger,
		knownLimiters: knownLimiters,
		registry:      registry,
		upgrader: websocket.Upgrader{
			ReadBufferSize:  4096,
			WriteBufferSize: 4096,
			// M-20: validate Origin against the explicit allow-list. Reject
			// empty and unknown origins for browser upgrades — never "*".
			CheckOrigin: func(r *http.Request) bool {
				origin := r.Header.Get("Origin")
				if origin == "" {
					// Browsers always send Origin on WS upgrades; an empty
					// Origin is a non-browser client and is not trusted.
					return false
				}
				return allowedOrigins[origin]
			},
		},
	}
}

// HandleLimiter handles GET /ws/v1/limiters/{algorithm}
func (ws *WSHandler) HandleLimiter(w http.ResponseWriter, r *http.Request) {
	algorithm := r.PathValue("algorithm")
	// L-12: validate the path param before upgrading.
	if err := validateKey(algorithm); err != nil {
		writeError(w, r, http.StatusBadRequest, "invalid_param", "invalid algorithm: "+err.Error())
		return
	}
	if !ws.knownLimiters[algorithm] {
		writeError(w, r, http.StatusNotFound, "not_found", "unknown algorithm: "+algorithm)
		return
	}
	ws.upgrade(w, r, algorithm)
}

// HandleCB handles GET /ws/v1/cb/{name}
func (ws *WSHandler) HandleCB(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	// L-12: validate the path param before upgrading.
	if err := validateKey(name); err != nil {
		writeError(w, r, http.StatusBadRequest, "invalid_param", "invalid name: "+err.Error())
		return
	}
	if ws.registry == nil || ws.registry.Get(name) == nil {
		writeError(w, r, http.StatusNotFound, "not_found", "unknown circuit breaker: "+name)
		return
	}
	ws.upgrade(w, r, name)
}

// HandleEvents handles GET /ws/v1/events — subscribes to all events.
func (ws *WSHandler) HandleEvents(w http.ResponseWriter, r *http.Request) {
	ws.upgrade(w, r, "")
}

func (ws *WSHandler) upgrade(w http.ResponseWriter, r *http.Request, filter string) {
	conn, err := ws.upgrader.Upgrade(w, r, nil)
	if err != nil {
		ws.logger.Warn("websocket upgrade failed", "error", err)
		return
	}

	// Enforce message size limit and initial read deadline.
	conn.SetReadLimit(maxWSMessageBytes)
	conn.SetReadDeadline(time.Now().Add(pongWait)) //nolint:errcheck
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(pongWait)) //nolint:errcheck
		return nil
	})

	client := &Client{
		hub:    ws.hub,
		conn:   conn,
		send:   make(chan []byte, 64),
		filter: filter,
	}

	ws.hub.register <- client

	// Send a welcome message using the standard envelope.
	welcome, _ := json.Marshal(Event{
		Type: "connected",
		Name: filter,
		Data: map[string]string{"filter": filter},
		TS:   time.Now().UnixMilli(),
	})
	client.send <- welcome

	// Start write pump in a goroutine; read pump in current goroutine
	go client.writePump()
	client.readPump()

	ws.hub.unregister <- client
}

// readPump drains incoming messages from the client (we don't process them,
// but we must read to handle ping/pong and detect disconnects).
func (c *Client) readPump() {
	defer c.conn.Close()
	for {
		_, _, err := c.conn.ReadMessage()
		if err != nil {
			break
		}
	}
}
