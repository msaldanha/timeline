package timeline

import (
	"context"

	"go.uber.org/zap"

	"github.com/msaldanha/setinstone/event"
)

type Watcher struct {
	tl     *Timeline
	evm    event.Manager
	logger *zap.Logger
}

func newWatcher(tl *Timeline) *Watcher {
	return &Watcher{tl: tl, evm: tl.evm, logger: tl.logger.Named("Watcher" + tl.addr.Address)}
}

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
		if v.Post != nil {
			callback(*v.Post)
		}

	})
}

func (w *Watcher) GetTimeline() *Timeline {
	return w.tl
}
