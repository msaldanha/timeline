package timeline

import (
	"context"

	"go.uber.org/zap"

	"github.com/msaldanha/setinstone/event"
)

// Watcher observes a timeline for events and provides callbacks for handling them.
// It allows registering callbacks for specific events like post additions.
type Watcher struct {
	tl     *Timeline
	evm    event.Manager
	logger *zap.Logger
}

func newWatcher(tl *Timeline) *Watcher {
	return &Watcher{tl: tl, evm: tl.evm, logger: tl.logger.Named("Watcher" + tl.addr.Address)}
}

// OnPostAdded registers a callback function that will be called when a new post is added to the timeline.
// The callback receives the Post that was added.
func (w *Watcher) OnPostAdded(callback func(post Post)) {
	w.evm.On(EventTypes.EventPostAdded, func(ev event.Event) {
		e, er := extractEvent(ev)
		if er != nil {
			return
		}
		v, found, er := w.tl.Get(context.Background(), e.Id)
		if er != nil {
			return
		}
		if !found {
			return
		}
		post, ok := v.Entry.(Post)
		if ok {
			callback(post)
		}

	})
}

// GetTimeline returns the Timeline instance that this Watcher is observing.
func (w *Watcher) GetTimeline() *Timeline {
	return w.tl
}
