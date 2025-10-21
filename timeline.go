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

// AddPost adds a post to the timeline and broadcasts a post-added event to any subscribers.
// It takes a context, the post to add, a root key, and a connector string.
// Returns the key of the added post and an error if the operation fails.
func (t *Timeline) AddPost(ctx context.Context, post Post, keyRoot, connector string) (string, error) {
	er := t.checkCanWrite()
	if er != nil {
		return "", er
	}
	post.Type = TypePost
	js, er := json.Marshal(post)
	if er != nil {
		return "", t.translateError(er)
	}
	i, er := t.gr.Append(ctx, keyRoot, graph.NodeData{Branch: connector, Branches: []string{ConnectorLike, ConnectorComment}, Data: js})
	if er != nil {
		return "", t.translateError(er)
	}
	t.broadcast(EventTypes.EventPostAdded, i.Key)
	return i.Key, nil
}

// AddLike adds a like reference to a post from another timeline and broadcasts a reference-added event.
// It also sends a referenced event to the target timeline.
// It takes a context, the like to add, a root key, and a connector string.
// Returns the key of the added reference and an error if the operation fails.
func (t *Timeline) AddLike(ctx context.Context, like Like) (string, error) {
	er := t.checkCanWrite()
	if er != nil {
		return "", er
	}
	like.Type = TypeLike
	itemFromOtherTimeline, _, er := t.Get(ctx, like.Target)
	if er != nil {
		return "", er
	}

	switch itemFromOtherTimeline.Entry.(type) {
	case Like, ReceivedLike:
		return "", ErrCannotRefARef
	}

	if itemFromOtherTimeline.Address == t.gr.GetAddress(ctx).Address {
		return "", ErrCannotRefOwnItem
	}

	if !t.canReceiveReference(itemFromOtherTimeline, ConnectorLike) {
		return "", ErrCannotAddReference
	}

	js, er := json.Marshal(like)
	if er != nil {
		return "", t.translateError(er)
	}
	i, er := t.gr.Append(ctx, "", graph.NodeData{Branch: ConnectorMain, Branches: []string{}, Data: js})
	if er != nil {
		return "", t.translateError(er)
	}
	t.broadcast(EventTypes.EventReferenceAdded, i.Key)
	t.sendEventToTimeline(itemFromOtherTimeline.Address, EventTypes.EventReferenced, i.Key, TypeLike)
	return i.Key, nil
}

// AddReceivedLike processes a like reference received from another timeline.
// It validates the reference, adds it to the timeline if valid, and broadcasts a reference-received event.
// It takes a context and the key of the reference.
// Returns the key of the processed reference and an error if the operation fails.
func (t *Timeline) AddReceivedLike(ctx context.Context, receivedLikeKey string) (string, error) {
	er := t.checkCanWrite()
	if er != nil {
		return "", er
	}
	itemFromOtherTimeline, found, er := t.Get(ctx, receivedLikeKey)
	if er != nil {
		return "", er
	}
	if !found {
		return "", ErrNotFound
	}

	receivedLike, ok := itemFromOtherTimeline.Entry.(Like)
	if !ok {
		return "", ErrNotAReference
	}

	if itemFromOtherTimeline.Address == t.gr.GetAddress(ctx).Address {
		return "", ErrCannotRefOwnItem
	}

	itemFromThisTimeline, found, er := t.Get(ctx, receivedLike.Target)
	if er != nil {
		return "", er
	}
	if !found {
		return "", ErrNotFound
	}

	switch itemFromThisTimeline.Entry.(type) {
	case Post, Comment:
	default:
		return "", ErrCannotAddReference
	}

	if itemFromThisTimeline.Address != t.gr.GetAddress(ctx).Address {
		return "", ErrCannotAddRefToNotOwnedItem
	}

	if !t.canReceiveReference(itemFromThisTimeline, ConnectorLike) {
		return "", ErrCannotAddReference
	}

	li := ReceivedLike{
		Target: itemFromThisTimeline.Key,
		Origin: itemFromOtherTimeline.Key,
		Base: Base{
			Type: TypeReceivedLike,
		},
	}
	js, er := json.Marshal(li)
	if er != nil {
		return "", t.translateError(er)
	}
	i, er := t.gr.Append(ctx, itemFromThisTimeline.Key, graph.NodeData{Branch: ConnectorLike, Branches: []string{}, Data: js})
	if er != nil {
		return "", t.translateError(er)
	}
	return i.Key, nil
}

// AddComment adds a comment reference to an item from another timeline and broadcasts events.
// It takes a context, the comment to add, a root key, and a connector name.
// Returns the key of the added reference and an error if the operation fails.
func (t *Timeline) AddComment(ctx context.Context, comment Comment) (string, error) {
	er := t.checkCanWrite()
	if er != nil {
		return "", er
	}
	comment.Type = TypeComment
	itemFromOtherTimeline, _, er := t.Get(ctx, comment.Target)
	if er != nil {
		return "", er
	}

	switch itemFromOtherTimeline.Entry.(type) {
	case Like, ReceivedLike:
		return "", ErrCannotAddReference
	}

	if itemFromOtherTimeline.Address == t.gr.GetAddress(ctx).Address {
		return "", ErrCannotRefOwnItem
	}

	if !t.canReceiveReference(itemFromOtherTimeline, ConnectorComment) {
		return "", ErrCannotAddReference
	}

	js, er := json.Marshal(comment)
	if er != nil {
		return "", t.translateError(er)
	}
	i, er := t.gr.Append(ctx, "", graph.NodeData{Branch: ConnectorMain, Branches: []string{ConnectorComment, ConnectorLike}, Data: js})
	if er != nil {
		return "", t.translateError(er)
	}
	t.broadcast(EventTypes.EventReferenceAdded, i.Key)
	t.sendEventToTimeline(itemFromOtherTimeline.Address, EventTypes.EventReferenced, i.Key, TypeComment)
	return i.Key, nil
}

// AddReceivedComment processes a comment reference received from another timeline.
// It validates the reference and records a ReceivedComment linked to the origin entry.
// Returns the key of the processed reference and an error if the operation fails.
func (t *Timeline) AddReceivedComment(ctx context.Context, receivedCommentKey string) (string, error) {
	er := t.checkCanWrite()
	if er != nil {
		return "", er
	}
	itemFromOtherTimeline, found, er := t.Get(ctx, receivedCommentKey)
	if er != nil {
		return "", er
	}
	if !found {
		return "", ErrNotFound
	}

	receivedComment, ok := itemFromOtherTimeline.Entry.(Comment)
	if !ok {
		return "", ErrNotAReference
	}

	if itemFromOtherTimeline.Address == t.gr.GetAddress(ctx).Address {
		return "", ErrCannotRefOwnItem
	}

	itemFromThisTimeline, found, er := t.Get(ctx, receivedComment.Target)
	if er != nil {
		return "", er
	}
	if !found {
		return "", ErrNotFound
	}

	switch itemFromThisTimeline.Entry.(type) {
	case Post, Comment:
	default:
		return "", ErrCannotAddReference
	}

	if itemFromThisTimeline.Address != t.gr.GetAddress(ctx).Address {
		return "", ErrCannotAddRefToNotOwnedItem
	}

	if !t.canReceiveReference(itemFromThisTimeline, ConnectorComment) {
		return "", ErrCannotAddReference
	}
	branches := []string{ConnectorComment, ConnectorLike}
	li := ReceivedComment{
		Target: itemFromThisTimeline.Key,
		Origin: itemFromOtherTimeline.Key,
		Base: Base{
			Type: TypeReceivedComment,
		},
	}
	js, er := json.Marshal(li)
	if er != nil {
		return "", t.translateError(er)
	}
	i, er := t.gr.Append(ctx, itemFromThisTimeline.Key, graph.NodeData{Branch: ConnectorComment, Branches: branches, Data: js})
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
// It takes a context, a root key, a starting key, an ending key, and the maximum number of items to retrieve.
// Returns a slice of items and an error if the retrieval fails.
// If count is less than or equal to 0, an empty slice is returned.
func (t *Timeline) GetFrom(ctx context.Context, keyRoot, keyFrom, keyTo string, count int) ([]Item, error) {
	var items = make([]Item, 0)
	er := t.getFrom(ctx, keyRoot, ConnectorMain, keyFrom, keyTo, count, true, func(item Item) {
		items = append(items, item)
	})
	return items, er
}

// GetLikes returns like entries connected to the given root item key.
// It traverses the like connector starting at key and returns up to count items
// between keyFrom and keyTo (inclusive), in the order provided by the underlying graph iterator.
func (t *Timeline) GetLikes(ctx context.Context, key, keyFrom, keyTo string, count int) ([]Item, error) {
	var items = make([]Item, 0)
	er := t.getFrom(ctx, key, ConnectorLike, keyFrom, keyTo, count, false, func(item Item) {
		items = append(items, item)
	})
	if er != nil {
		return items, er
	}
	return items, er
}

// GetComments returns comment entries connected to the given root item key.
// It traverses the comment connector starting at key and returns up to count items
// between keyFrom and keyTo (inclusive), in the order provided by the underlying graph iterator.
func (t *Timeline) GetComments(ctx context.Context, key, keyFrom, keyTo string, count int) ([]Item, error) {
	var items = make([]Item, 0)
	er := t.getFrom(ctx, key, ConnectorComment, keyFrom, keyTo, count, false, func(item Item) {
		items = append(items, item)
	})
	if er != nil {
		return items, er
	}
	return items, er
}

func (t *Timeline) getFrom(ctx context.Context, keyRoot, connector, keyFrom, keyTo string, count int, includeRootNode bool,
	callback func(item Item)) error {
	if callback == nil {
		return nil
	}
	it := t.gr.GetIterator(ctx, keyRoot, connector, keyFrom)
	i := 0
	for v := range it.All() {
		item, er := NewItemFromGraphNode(*v)
		if er != nil {
			return t.translateError(er)
		}
		if !includeRootNode && item.Key == keyRoot {
			break
		}
		callback(item)
		i++
		if v.Key == keyTo || i >= count {
			break
		}
	}
	return nil
}

func (t *Timeline) canReceiveReference(item Item, con string) bool {
	found := false
	for _, connector := range item.GetAllowedReferences() {
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
		t.logger.Error("Failed to extract event", zap.Error(er))
		return
	}
	t.logger.Info("Received reference", zap.String("type", v.Type), zap.String("id", v.Id), zap.String("referenceType", v.ReferenceType))

	switch v.ReferenceType {
	case TypeLike:
		_, er = t.AddReceivedLike(context.Background(), v.Id)
		if er != nil {
			t.logger.Error("Failed to add received like", zap.String("id", v.Id), zap.Error(er))
		}
	case TypeComment:
		_, er = t.AddReceivedComment(context.Background(), v.Id)
		if er != nil {
			t.logger.Error("Failed to add received comment", zap.String("id", v.Id), zap.Error(er))
		}
	default:
		t.logger.Warn("Unknown reference type", zap.String("referenceType", v.ReferenceType), zap.String("id", v.Id))
	}
}

func (t *Timeline) broadcast(eventType, eventValue string) {
	ev := Event{
		Type: eventType,
		Id:   eventValue,
	}
	_ = t.evm.Emit(eventType, ev.ToJson())
}

func (t *Timeline) sendEventToTimeline(addr, eventType, eventValue, referenceType string) {
	evm, er := t.getEvmForTimeline(addr)
	if er != nil {
		t.logger.Error("Unable to get event manager", zap.String("addr", addr), zap.Error(er))
		return
	}
	ev := Event{
		Type:          eventType,
		Id:            eventValue,
		ReferenceType: referenceType,
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
