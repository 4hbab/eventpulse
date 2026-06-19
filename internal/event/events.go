package event

import "time"

// Routing key prefixes -> Category of a tracking event
type EventType string

const (
	PageView EventType = "event.pageview"
	Click    EventType = "event.click"
	Signup   EventType = "event.signup"
	Purchase EventType = "event.purchase"
)

// Will be used for randomly selecting a topic to publish to
func AllTypes() []EventType {
	return []EventType{
		PageView, Click, Signup, Purchase,
	}
}

// core message payload sent to RabbitMQ
// This will get serialized to JSON/Protobuf and published
type TrackingEvent struct {
	ID        string         `json:"id"`
	Type      EventType      `json:"type"`
	UserID    string         `json:"user_id"`
	SessionID string         `json:"session_id"`
	Page      string         `json:"page"`
	Metadata  map[string]any `json:"metadata,omitempty"`
	Timestamp time.Time      `json:"timestamp"`
}
