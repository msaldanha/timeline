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

	"github.com/msaldanha/setinstone/address"
	"github.com/msaldanha/setinstone/event"
	"github.com/msaldanha/setinstone/graph"
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
	isRunning        atomic.Bool
	ctx              context.Context
	cancelRunCtxFunc context.CancelFunc
}

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
		isRunning:        atomic.Bool{},
		ctx:              ctx,
		cancelRunCtxFunc: cancel,
	}, nil
}

func (ct *CompositeTimeline) Init() error {
	er := ct.dao.Init()
	if er != nil {
		return er
	}

	ct.initialized = true
	return nil
}

func (ct *CompositeTimeline) Refresh() error {
	if !ct.initialized {
		return ErrNotInitialized
	}
	_, er := ct.loadMore(defaultCount, true)
	return er
}

func (ct *CompositeTimeline) Rebuild() error {
	if !ct.initialized {
		return ErrNotInitialized
	}
	_, er := ct.loadMore(defaultCount, false)
	return er
}

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

func (ct *CompositeTimeline) LoadTimeline(addr *address.Address) error {
	if !ct.initialized {
		return ErrNotInitialized
	}

	if addr == nil || addr.Address == emptyString {
		return ErrInvalidParameterAddress
	}
	gr := graph.New(ct.ns, addr, ct.node, ct.logger)
	tl, er := newTimeline(ct.ns, addr, gr, ct.evmf, ct.logger)
	if er != nil {
		return er
	}

	watcher := newWatcher(tl)
	watcher.OnPostAdded(ct.onPostAdded)
	ct.criticalSession(func() {
		ct.watchers[tl.addr.Address] = watcher
	})
	return nil
}

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

func (ct *CompositeTimeline) Get(ctx context.Context, key string) (Item, bool, error) {
	return ct.dao.Get(ctx, key)
}

func (ct *CompositeTimeline) onPostAdded(post Post) {

}

func (ct *CompositeTimeline) Save(item Item) error {
	return ct.dao.Put(item)
}

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
		items, err := tl.GetFrom(context.Background(), emptyString, mainBranch, tlLastKey, emptyString, totalToRetrieve)
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
