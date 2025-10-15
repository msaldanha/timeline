package timeline

import (
	"encoding/json"

	"github.com/msaldanha/setinstone/event"
)

// EventTypesEnum defines the event names emitted and consumed by the timeline package.
// These constants are used to subscribe and publish timeline-related events.
// See EventTypes for the default values.
type EventTypesEnum struct {
	EventReferenced     string
	EventReferenceAdded string
	EventPostAdded      string
}

// EventTypes contains the concrete event names used by this package.
// Consumers can reference these strings to subscribe to timeline events.
var EventTypes = EventTypesEnum{
	EventReferenced:     "TIMELINE.EVENT.REFERENCED",
	EventReferenceAdded: "TIMELINE.EVENT.REFERENCE.ADDED",
	EventPostAdded:      "TIMELINE.EVENT.POST.ADDED",
}

// Event is the canonical payload for timeline events.
// Type identifies the kind of event (see EventTypes) and Id carries the related entity key.
type Event struct {
	Type string `json:"type,omitempty"`
	Id   string `json:"id,omitempty"`
}

// Bytes returns a byte slice that uniquely represents the event
// by concatenating its Type and Id. Useful for hashing or de-duplication.
func (e Event) Bytes() []byte {
	return []byte(e.Type + e.Id)
}

// ToJson returns the JSON encoding of the Event.
// Errors during marshaling are ignored and result in an empty byte slice.
func (e Event) ToJson() []byte {
	b, _ := json.Marshal(e)
	return b
}

func extractEvent(ev event.Event) (Event, error) {
	v := Event{}
	er := json.Unmarshal(ev.Data(), &v)
	if er != nil {
		return Event{}, er
	}
	return v, nil
}
