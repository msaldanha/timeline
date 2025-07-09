package timeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"go.uber.org/zap"

	"github.com/msaldanha/setinstone/address"
	"github.com/msaldanha/setinstone/cache"
	"github.com/msaldanha/setinstone/event"
	"github.com/msaldanha/setinstone/graph"
)

// Timeline represents a sequence of items (posts and references) that can be appended to and retrieved.
// It provides functionality for adding posts, creating references to posts in other timelines,
// and retrieving items from the timeline.
type Timeline struct {
	gr        Graph
	evm       event.Manager
	evmf      event.ManagerFactory
	ns        string
	addr      *address.Address
	evmsCache cache.Cache[event.Manager]
	logger    *zap.Logger
}

// NewTimeline creates a new Timeline instance.
// It takes a namespace, an address, a graph implementation, an event manager factory, and a logger.
// Returns a new Timeline instance and an error if creation fails.
func NewTimeline(ns string, addr *address.Address, gr Graph, evmf event.ManagerFactory, logger *zap.Logger) (*Timeline, error) {
	return newTimeline(ns, addr, gr, evmf, logger)
}

func newTimeline(ns string, addr *address.Address, gr Graph, evmf event.ManagerFactory, logger *zap.Logger) (*Timeline, error) {

	if gr == nil {
		return nil, ErrInvalidParameterGraph
	}

	if evmf == nil {
		return nil, ErrInvalidParameterEventManager
	}

	logger = logger.Named("Timeline").With(zap.String("namespace", ns), zap.String("addr", addr.Address))

	evm, er := evmf.Build(addr, addr, logger)
	if er != nil {
		return nil, er
	}

	evmsCache := cache.NewMemoryCache[event.Manager](0)

	tl := &Timeline{
		gr:        gr,
		evm:       evm,
		evmf:      evmf,
		ns:        ns,
		addr:      addr,
		evmsCache: evmsCache,
		logger:    logger,
	}

	_ = evm.On(EventTypes.EventReferenced, tl.refAddedHandler)

	return tl, nil
}

// AppendPost adds a post to the timeline and broadcasts a post-added event to any subscribers.
// It takes a context, the post to add, a root key, and a connector string.
// Returns the key of the added post and an error if the operation fails.
func (t *Timeline) AppendPost(ctx context.Context, post Post, keyRoot, connector string) (string, error) {
	er := t.checkCanWrite()
	if er != nil {
		return "", er
	}
	post.Type = TypePost
	js, er := json.Marshal(post)
	if er != nil {
		return "", t.translateError(er)
	}
	i, er := t.gr.Append(ctx, keyRoot, graph.NodeData{Branch: connector, Branches: post.Connectors, Data: js})
	if er != nil {
		return "", t.translateError(er)
	}
	t.broadcast(EventTypes.EventPostAdded, i.Key)
	return i.Key, nil
}

// AppendReference adds a reference to a post from another timeline and broadcasts a reference-added event.
// It also sends a referenced event to the target timeline.
// It takes a context, the reference to add, a root key, and a connector string.
// Returns the key of the added reference and an error if the operation fails.
func (t *Timeline) AppendReference(ctx context.Context, ref Reference, keyRoot, connector string) (string, error) {
	er := t.checkCanWrite()
	if er != nil {
		return "", er
	}
	ref.Type = TypeReference
	v, _, er := t.Get(ctx, ref.Target)
	if er != nil {
		return "", er
	}
	if v.Reference != nil {
		return "", ErrCannotRefARef
	}

	if v.Address == t.gr.GetAddress(ctx).Address {
		return "", ErrCannotRefOwnItem
	}

	if !t.canReceiveReference(v, ref.Connector) {
		return "", ErrCannotAddReference
	}

	mi := Reference{
		Connector: ref.Connector,
		Target:    ref.Target,
		Base: Base{
			Type: TypeReference,
		},
	}
	js, er := json.Marshal(mi)
	if er != nil {
		return "", t.translateError(er)
	}
	i, er := t.gr.Append(ctx, keyRoot, graph.NodeData{Branch: connector, Data: js})
	if er != nil {
		return "", t.translateError(er)
	}
	t.broadcast(EventTypes.EventReferenceAdded, i.Key)
	t.sendEventToTimeline(v.Address, EventTypes.EventReferenced, i.Key)
	return i.Key, nil
}

// AddReceivedReference processes a reference received from another timeline.
// It validates the reference, adds it to the timeline if valid, and broadcasts a reference-received event.
// It takes a context and the key of the reference.
// Returns the key of the processed reference and an error if the operation fails.
func (t *Timeline) AddReceivedReference(ctx context.Context, refKey string) (string, error) {
	er := t.checkCanWrite()
	if er != nil {
		return "", er
	}
	item, found, er := t.Get(ctx, refKey)
	if er != nil {
		return "", er
	}
	if !found {
		return "", ErrNotFound
	}

	receivedRef := item.Reference
	if receivedRef == nil {
		return "", ErrNotAReference
	}

	if item.Address == t.gr.GetAddress(ctx).Address {
		return "", ErrCannotRefOwnItem
	}

	item, found, er = t.Get(ctx, receivedRef.Target)
	if er != nil {
		return "", er
	}
	if !found {
		return "", ErrNotFound
	}
	if item.Post == nil {
		return "", ErrCannotAddReference
	}

	if item.Address != t.gr.GetAddress(ctx).Address {
		return "", ErrCannotAddRefToNotOwnedItem
	}

	if !t.canReceiveReference(item, receivedRef.Connector) {
		return "", ErrCannotAddReference
	}

	li := Reference{
		Target:    refKey,
		Connector: receivedRef.Connector,
		Base: Base{
			Type: TypeReference,
		},
	}
	js, er := json.Marshal(li)
	if er != nil {
		return "", t.translateError(er)
	}
	i, er := t.gr.Append(ctx, item.Key, graph.NodeData{Branch: receivedRef.Connector, Data: js})
	if er != nil {
		return "", t.translateError(er)
	}
	return i.Key, nil
}

// Get retrieves a single item from the timeline by its key.
// It takes a context and a key to identify the item.
// Returns the item, a boolean indicating whether the item was found, and an error if the retrieval fails.
func (t *Timeline) Get(ctx context.Context, key string) (Item, bool, error) {
	v, found, er := t.gr.Get(ctx, key)
	if er != nil {
		return Item{}, false, t.translateError(er)
	}
	i, er := NewItemFromGraphNode(v)
	if er != nil {
		return Item{}, false, t.translateError(er)
	}
	return i, found, nil
}

// GetFrom retrieves multiple items from the timeline starting from a specific key.
// It takes a context, a root key, a connector string, a starting key, an ending key, and the maximum number of items to retrieve.
// Returns a slice of items and an error if the retrieval fails.
// If count is less than or equal to 0, an empty slice is returned.
func (t *Timeline) GetFrom(ctx context.Context, keyRoot, connector, keyFrom, keyTo string, count int) ([]Item, error) {
	it := t.gr.GetIterator(ctx, keyRoot, connector, keyFrom)
	i := 0
	var items = make([]Item, 0)
	for v := range it.All() {
		item, er := NewItemFromGraphNode(*v)
		if er != nil {
			return nil, t.translateError(er)
		}
		items = append(items, item)
		i++
		if v.Key == keyTo || i >= count {
			break
		}
	}
	return items, nil
}

func (t *Timeline) canReceiveReference(item Item, con string) bool {
	found := false
	for _, connector := range item.Branches {
		if connector == con {
			found = true
			break
		}
	}
	return found
}

func (t *Timeline) translateError(er error) error {
	switch {
	case errors.Is(er, graph.ErrReadOnly):
		return ErrReadOnly
	case errors.Is(er, graph.ErrNotFound):
		return ErrNotFound
	default:
		return fmt.Errorf("unable to process the request: %w", er)
	}
}

func (t *Timeline) refAddedHandler(ev event.Event) {
	v, er := t.extractEvent(ev)
	if er != nil {
		return
	}
	t.logger.Info("Received reference", zap.String("type", v.Type), zap.String("id", v.Id))
	_, _ = t.AddReceivedReference(context.Background(), v.Id)
}

func (t *Timeline) broadcast(eventType, eventValue string) {
	ev := Event{
		Type: eventType,
		Id:   eventValue,
	}
	_ = t.evm.Emit(eventType, ev.ToJson())
}

func (t *Timeline) sendEventToTimeline(addr, eventType, eventValue string) {
	evm, er := t.getEvmForTimeline(addr)
	if er != nil {
		t.logger.Error("Unable to get event manager", zap.String("addr", addr), zap.Error(er))
		return
	}
	ev := Event{
		Type: eventType,
		Id:   eventValue,
	}
	_ = evm.Emit(eventType, ev.ToJson())
}

func (t *Timeline) getEvmForTimeline(addr string) (event.Manager, error) {
	evm, found, er := t.evmsCache.Get(addr)
	if er != nil {
		return nil, er
	}
	if found {
		return evm, nil
	}
	evm, er = t.evmf.Build(t.addr, &address.Address{Address: addr}, t.logger)
	if er != nil {
		return nil, er
	}
	_ = t.evmsCache.Add(addr, evm)
	return evm, nil
}

func (t *Timeline) extractEvent(ev event.Event) (Event, error) {
	logger := t.logger.With(zap.String("name", ev.Name()), zap.String("data", string(ev.Data())))
	logger.Info("Received event")

	v := Event{}
	er := json.Unmarshal(ev.Data(), &v)
	if er != nil {
		logger.Error("Invalid event received", zap.Error(er))
		return Event{}, er
	}
	logger.Info("Timeline event received", zap.String("type", v.Type), zap.String("id", v.Id))
	return v, nil
}

func (t *Timeline) checkCanWrite() error {
	if t.addr == nil || !t.addr.HasKeys() {
		return ErrReadOnly
	}
	return nil
}
