package timeline

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ipfs/kubo/core"
	bolt "go.etcd.io/bbolt"
	"go.uber.org/zap"

	"github.com/msaldanha/setinstone/event"
)

const (
	timelineBucketName       = "timeline"
	timelineIndexBucketName  = "timelineIndex"
	compositeBucketName      = "compositeTimeline"
	lastAddressKeyBucketName = "lastAddressKey"
	defaultCount             = 20

	// Logger related constants
	loggerNameCompositeTimeline = "CompositeTimeline"
	loggerFieldNamespace        = "namespace"
	loggerFieldOwner            = "owner"

	// Other string constants
	emptyString = ""
	mainBranch  = "main"
)

var (
	ErrNotInitialized = errors.New("not initialized")
)

// CompositeTimeline manages multiple timelines and combines their content into a single timeline.
// It provides functionality to load, add, and remove timelines, as well as to retrieve and manipulate
// timeline items.
type CompositeTimeline struct {
	watchers         map[string]*Watcher
	mtx              *sync.Mutex
	initialized      bool
	node             *core.IpfsNode
	evm              event.Manager
	evmf             event.ManagerFactory
	ns               string
	logger           *zap.Logger
	owner            string
	dao              Dao
	runOnce          sync.Once
	stopOnce         sync.Once
	initOnce         sync.Once
	initErr          error
	isRunning        atomic.Bool
	ctx              context.Context
	cancelRunCtxFunc context.CancelFunc
}

// NewCompositeTimeline creates a new CompositeTimeline instance.
// It takes a namespace, an IPFS node, an event manager factory, a logger, an owner identifier, and a data access object.
// Returns a new CompositeTimeline instance and an error if the event manager factory is nil.
func NewCompositeTimeline(ns string, node *core.IpfsNode, evmf event.ManagerFactory, logger *zap.Logger, owner string, dao Dao) (*CompositeTimeline, error) {
	if evmf == nil {
		return nil, ErrInvalidParameterEventManager
	}

	logger = logger.Named(loggerNameCompositeTimeline).With(zap.String(loggerFieldNamespace, ns), zap.String(loggerFieldOwner, owner))
	ctx, cancel := context.WithCancel(context.Background())
	return &CompositeTimeline{
		watchers:         make(map[string]*Watcher),
		mtx:              new(sync.Mutex),
		initialized:      false,
		node:             node,
		ns:               ns,
		evmf:             evmf,
		logger:           logger,
		owner:            owner,
		dao:              dao,
		runOnce:          sync.Once{},
		stopOnce:         sync.Once{},
		initOnce:         sync.Once{},
		initErr:          nil,
		isRunning:        atomic.Bool{},
		ctx:              ctx,
		cancelRunCtxFunc: cancel,
	}, nil
}

// Init initializes the CompositeTimeline.
// It initializes the data access object and sets the initialized flag to true.
// Returns an error if the initialization fails.
func (ct *CompositeTimeline) Init() error {
	ct.initOnce.Do(func() {
		ct.initErr = ct.dao.Init()
		if ct.initErr != nil {
			return
		}
		ct.initialized = true
	})

	return ct.initErr
}

// Refresh updates the composite timeline by loading more items from the underlying timelines.
// It returns an error if the CompositeTimeline is not initialized or if loading more items fails.
func (ct *CompositeTimeline) Refresh() error {
	if !ct.initialized {
		return ErrNotInitialized
	}
	_, er := ct.loadMore(defaultCount, true)
	return er
}

// Rebuild reconstructs the composite timeline by loading items from the underlying timelines.
// Unlike Refresh, it starts from the beginning rather than continuing from the last known point.
// Returns an error if the CompositeTimeline is not initialized or if loading items fails.
func (ct *CompositeTimeline) Rebuild() error {
	if !ct.initialized {
		return ErrNotInitialized
	}
	_, er := ct.loadMore(defaultCount, false)
	return er
}

// Run starts a background process that periodically refreshes the composite timeline.
// The refresh happens every 10 seconds until Stop is called.
// Returns an error if the CompositeTimeline is not initialized.
// This method is idempotent and will only start the background process once.
func (ct *CompositeTimeline) Run() error {
	if !ct.initialized {
		return ErrNotInitialized
	}

	ct.runOnce.Do(func() {
		ct.isRunning.Store(true)
		go func() {
			tk := time.NewTicker(time.Second * 10)
			defer tk.Stop()
			defer ct.isRunning.Store(false)
			for {
				select {
				case <-tk.C:
					ct.Refresh()
				case <-ct.ctx.Done():
					break
				}
			}
		}()
	})
	return nil
}

// Stop halts the background refresh process started by Run.
// Returns an error if the CompositeTimeline is not initialized.
// This method is idempotent and will only stop the background process once.
// If the background process is not running, this method does nothing and returns nil.
func (ct *CompositeTimeline) Stop() error {
	if !ct.initialized {
		return ErrNotInitialized
	}
	if !ct.isRunning.Load() {
		return nil
	}
	ct.stopOnce.Do(func() {
		ct.cancelRunCtxFunc()
	})
	return nil
}

// AddTimeline adds an existing timeline to the composite timeline.
// It sets up a watcher for the timeline and registers the watcher with the composite timeline.
// Returns an error if the CompositeTimeline is not initialized.
func (ct *CompositeTimeline) AddTimeline(tl *Timeline) error {
	if !ct.initialized {
		return ErrNotInitialized
	}

	watcher := newWatcher(tl)
	watcher.OnPostAdded(ct.onPostAdded)
	ct.criticalSession(func() {
		ct.watchers[tl.addr.Address] = watcher
	})
	return nil
}

// RemoveTimeline removes a timeline with the given address from the composite timeline.
// It deletes the last key for the address from the data store and removes the watcher for the timeline.
// Returns an error if deleting the last key fails.
func (ct *CompositeTimeline) RemoveTimeline(addr string) error {
	er := ct.dao.DeleteLastKeyForAddress(addr)
	if er != nil {
		return er
	}
	ct.criticalSession(func() {
		delete(ct.watchers, addr)
	})
	return nil
}

// GetFrom retrieves items from the composite timeline starting from the given key.
// It takes a context (which is currently not used), a starting key, and the maximum number of items to retrieve.
// Returns a slice of items and an error if the CompositeTimeline is not initialized or if reading fails.
// If count is less than or equal to 0, an empty slice is returned.
func (ct *CompositeTimeline) GetFrom(_ context.Context, keyFrom string, count int) ([]Item, error) {
	if !ct.initialized {
		return nil, ErrNotInitialized
	}
	if count <= 0 {
		return []Item{}, nil
	}
	results, er := ct.readFrom(keyFrom, count)
	if er != nil {
		return nil, er
	}
	return results, nil
}

// Get retrieves a single item from the composite timeline by its key.
// It takes a context and a key to identify the item.
// Returns the item, a boolean indicating whether the item was found, and an error if the retrieval fails.
func (ct *CompositeTimeline) Get(ctx context.Context, key string) (Item, bool, error) {
	return ct.dao.Get(ctx, key)
}

func (ct *CompositeTimeline) onPostAdded(post Post) {

}

// Save stores an item in the composite timeline.
// It takes an item to store and delegates the operation to the data access object.
// Returns an error if the save operation fails.
func (ct *CompositeTimeline) Save(item Item) error {
	return ct.dao.Put(item)
}

// Clear removes all items from the composite timeline.
// It delegates the operation to the data access object.
// Returns an error if the clear operation fails.
func (ct *CompositeTimeline) Clear() error {
	return ct.dao.DeleteAll()
}
func (ct *CompositeTimeline) getLastKeyForAddress(address string) string {
	return ct.dao.GetLastKeyForAddress(address)
}

func (ct *CompositeTimeline) criticalSession(session func()) {
	ct.mtx.Lock()
	defer ct.mtx.Unlock()
	session()
}

func (ct *CompositeTimeline) getTimelineBuckets(tx *bolt.Tx) (tl *bolt.Bucket, tlIndex *bolt.Bucket, lastKey *bolt.Bucket) {
	comp := tx.Bucket([]byte(compositeBucketName))
	own := comp.Bucket([]byte(ct.owner))
	tl = own.Bucket([]byte(timelineBucketName))
	tlIndex = own.Bucket([]byte(timelineIndexBucketName))
	lastKey = own.Bucket([]byte(lastAddressKeyBucketName))
	return
}

func (ct *CompositeTimeline) createTimelineBuckets(tx *bolt.Tx) error {
	comp, er := tx.CreateBucketIfNotExists([]byte(compositeBucketName))
	if er != nil {
		return er
	}

	own, er := comp.CreateBucketIfNotExists([]byte(ct.owner))
	if er != nil {
		return er
	}

	_, er = own.CreateBucketIfNotExists([]byte(lastAddressKeyBucketName))
	if er != nil {
		return er
	}
	_, er = own.CreateBucketIfNotExists([]byte(timelineBucketName))
	if er != nil {
		return er
	}
	_, er = own.CreateBucketIfNotExists([]byte(timelineIndexBucketName))
	if er != nil {
		return er
	}
	return nil
}

func (ct *CompositeTimeline) loadMore(count int, getOlder bool) ([]Item, error) {
	watchers := make(map[string]*Watcher)
	ct.criticalSession(func() {
		for k, w := range ct.watchers {
			watchers[k] = w
		}
	})
	totalToRetrieve := count
	if defaultCount > totalToRetrieve {
		totalToRetrieve = defaultCount
	}
	allItems := make([]Item, 0, len(watchers)*defaultCount)
	for k, w := range watchers {
		tl := w.GetTimeline()
		tlLastKey := emptyString
		if getOlder {
			tlLastKey = ct.getLastKeyForAddress(k)
		}
		items, err := tl.GetFrom(context.Background(), emptyString, mainBranch, emptyString, tlLastKey, totalToRetrieve)
		if err != nil {
			return nil, err
		}
		for _, item := range items {
			_, found, er := ct.Get(context.Background(), item.Key)
			if er != nil {
				return nil, er
			}
			if found {
				continue
			}
			allItems = append(allItems, item)
		}
	}
	for _, item := range allItems {
		_ = ct.Save(item)
	}
	return allItems, nil
}

func (ct *CompositeTimeline) readFrom(keyFrom string, count int) ([]Item, error) {
	if !ct.initialized {
		return nil, ErrNotInitialized
	}
	if count <= 0 {
		return []Item{}, nil
	}
	results := make([]Item, 0, count)
	iter, er := ct.dao.GetIterator(keyFrom)
	if er != nil {
		return nil, er
	}
	c := 1
	for item, er := iter.First(); er == nil && item != nil && c <= count; item, er = iter.Next() {
		results = append(results, *item)
		c++
	}
	return results, nil
}
