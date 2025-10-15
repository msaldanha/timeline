package timeline_test

import (
	"context"
	"encoding/json"
	"fmt"
	"iter"
	"strconv"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"

	"github.com/msaldanha/setinstone/address"
	"github.com/msaldanha/setinstone/event"
	"github.com/msaldanha/setinstone/graph"

	"github.com/msaldanha/timeline"
)

const (
	likeRef = "like"
)

var _ = Describe("Timeline", func() {

	var ctx context.Context

	addr, _ := address.NewAddressWithKeys()
	ns := "test"

	logger := zap.NewNop()

	BeforeEach(func() {
		ctx = context.Background()
	})

	It("Should add a post", func() {
		mockCtrl := gomock.NewController(GinkgoT())
		defer mockCtrl.Finish()

		gr := timeline.NewMockGraph(mockCtrl)
		gr.EXPECT().Append(gomock.Any(), gomock.Any(), gomock.Any()).Return(graph.Node{Key: "key"}, nil)

		evf, evm := createMockFactoryAndManager(mockCtrl, ns)

		evm.EXPECT().On(timeline.EventTypes.EventReferenced, gomock.Any()).Return(&event.Subscription{})
		evm.EXPECT().Emit("TIMELINE.EVENT.POST.ADDED", gomock.Any()).Return(nil)

		p, _ := timeline.NewTimeline(ns, addr, gr, evf, logger)

		post := timeline.Post{Part: timeline.Part{MimeType: "plain/text", Body: "some text"}}
		key, er := p.AddPost(ctx, post, "", "main")
		Expect(er).To(BeNil())
		Expect(key).ToNot(Equal(""))
	})

	It("Should get post by key", func() {
		mockCtrl := gomock.NewController(GinkgoT())
		defer mockCtrl.Finish()

		expectedPost := timeline.Post{Base: timeline.Base{Type: timeline.TypePost}, Part: timeline.Part{MimeType: "plain/text", Body: "some text"}}

		gr := timeline.NewMockGraph(mockCtrl)
		data, _ := json.Marshal(expectedPost)
		gr.EXPECT().Get(gomock.Any(), gomock.Any()).Return(graph.Node{Key: "key", Data: data}, true, nil)

		evf, evm := createMockFactoryAndManager(mockCtrl, ns)

		evm.EXPECT().On(timeline.EventTypes.EventReferenced, gomock.Any()).Return(&event.Subscription{})

		p, _ := timeline.NewTimeline(ns, addr, gr, evf, logger)

		i, found, er := p.Get(ctx, "key")
		Expect(er).To(BeNil())
		Expect(found).To(BeTrue())
		postItem, _ := i.Entry.(timeline.Post)
		Expect(postItem.Part).To(Equal(expectedPost.Part))
	})

	It("Should add a received reference", func() {
		mockCtrl := gomock.NewController(GinkgoT())
		defer mockCtrl.Finish()

		evf, evm := createMockFactoryAndManager(mockCtrl, ns)
		evm.EXPECT().On(timeline.EventTypes.EventReferenced, gomock.Any()).Return(&event.Subscription{})
		gr := timeline.NewMockGraph(mockCtrl)

		tl1, _ := timeline.NewTimeline(ns, addr, gr, evf, logger)

		likeKey := "likeKey"
		postKey := "postKey"
		referenceKey := "refKey"
		expectedPost := timeline.Post{
			Base: timeline.Base{Type: timeline.TypePost, Connectors: []string{likeRef}},
			Part: timeline.Part{MimeType: "plain/text", Body: "some text"},
		}
		postjson, _ := json.Marshal(expectedPost)
		expectedLike := timeline.Like{
			Base:   timeline.Base{Type: timeline.TypeLike, Connectors: []string{likeRef}},
			Target: postKey, Connector: likeRef}
		likejson, _ := json.Marshal(expectedLike)
		gr.EXPECT().Get(gomock.Any(), likeKey).Return(graph.Node{Key: likeKey, Data: likejson, Branches: []string{likeRef}}, true, nil)
		gr.EXPECT().GetAddress(gomock.Any()).Return(addr)
		gr.EXPECT().Get(gomock.Any(), postKey).Return(graph.Node{Key: postKey, Address: addr.Address, Data: postjson, Branches: []string{likeRef}}, true, nil)
		gr.EXPECT().GetAddress(gomock.Any()).Return(addr)
		gr.EXPECT().Append(gomock.Any(), gomock.Any(), gomock.Any()).Return(graph.Node{Key: referenceKey}, nil)

		receivedKey, er := tl1.AddReceivedLike(ctx, likeKey)
		Expect(er).To(BeNil())
		Expect(receivedKey).To(Equal(referenceKey))
	})

	It("Should add a received comment", func() {
		mockCtrl := gomock.NewController(GinkgoT())
		defer mockCtrl.Finish()

		evf, evm := createMockFactoryAndManager(mockCtrl, ns)
		evm.EXPECT().On(timeline.EventTypes.EventReferenced, gomock.Any()).Return(&event.Subscription{})
		gr := timeline.NewMockGraph(mockCtrl)

		tl1, _ := timeline.NewTimeline(ns, addr, gr, evf, logger)

		commentKey := "commentKey"
		postKey := "postKey"
		referenceKey := "refKey"
		commentRef := "comment"
		expectedPost := timeline.Post{
			Base: timeline.Base{Type: timeline.TypePost, Connectors: []string{commentRef}},
			Part: timeline.Part{MimeType: "plain/text", Body: "some text"},
		}
		postjson, _ := json.Marshal(expectedPost)
		expectedComment := timeline.Comment{
			Post: timeline.Post{
				Base: timeline.Base{Type: timeline.TypeComment, Connectors: []string{commentRef}},
				Part: timeline.Part{MimeType: "plain/text", Body: "comment text"},
			},
			Target:    postKey,
			Connector: commentRef,
		}
		commentjson, _ := json.Marshal(expectedComment)
		gr.EXPECT().Get(gomock.Any(), commentKey).Return(graph.Node{Key: commentKey, Data: commentjson, Branches: []string{commentRef}}, true, nil)
		gr.EXPECT().GetAddress(gomock.Any()).Return(addr)
		gr.EXPECT().Get(gomock.Any(), postKey).Return(graph.Node{Key: postKey, Address: addr.Address, Data: postjson, Branches: []string{commentRef}}, true, nil)
		gr.EXPECT().GetAddress(gomock.Any()).Return(addr)
		gr.EXPECT().Append(gomock.Any(), gomock.Any(), gomock.Any()).Return(graph.Node{Key: referenceKey}, nil)

		receivedKey, er := tl1.AddReceivedComment(ctx, commentKey)
		Expect(er).To(BeNil())
		Expect(receivedKey).To(Equal(referenceKey))
	})

	It("Should NOT append reference to own reference", func() {
		mockCtrl := gomock.NewController(GinkgoT())
		defer mockCtrl.Finish()

		gr := timeline.NewMockGraph(mockCtrl)

		evf, evm := createMockFactoryAndManager(mockCtrl, ns)
		evm.EXPECT().On(timeline.EventTypes.EventReferenced, gomock.Any()).Return(&event.Subscription{})

		p, _ := timeline.NewTimeline(ns, addr, gr, evf, logger)

		postKey := "postKey"
		expectedPost := timeline.Post{
			Base: timeline.Base{Type: timeline.TypePost, Connectors: []string{likeRef}},
			Part: timeline.Part{MimeType: "plain/text", Body: "some text"},
		}
		postjson, _ := json.Marshal(expectedPost)
		gr.EXPECT().Get(gomock.Any(), postKey).Return(graph.Node{Key: postKey, Address: addr.Address, Data: postjson, Branches: []string{likeRef}}, true, nil)
		gr.EXPECT().GetAddress(gomock.Any()).Return(addr)
		expectedLike := timeline.Like{Target: postKey, Connector: "connector"}
		key, er := p.AddLike(ctx, expectedLike, "", "main")
		Expect(er).To(Equal(timeline.ErrCannotRefOwnItem))
		Expect(key).To(Equal(""))

	})

	It("Should NOT append a reference to reference", func() {
		mockCtrl := gomock.NewController(GinkgoT())
		defer mockCtrl.Finish()

		gr := timeline.NewMockGraph(mockCtrl)

		evf, evm := createMockFactoryAndManager(mockCtrl, ns)
		evm.EXPECT().On(timeline.EventTypes.EventReferenced, gomock.Any()).Return(&event.Subscription{})

		p, _ := timeline.NewTimeline(ns, addr, gr, evf, logger)

		postKey := "postKey"
		likeKey := "likeKey"
		expectedLike := timeline.Like{
			Base:   timeline.Base{Type: timeline.TypeLike, Connectors: []string{likeRef}},
			Target: postKey, Connector: likeRef}
		likejson, _ := json.Marshal(expectedLike)
		gr.EXPECT().Get(gomock.Any(), likeKey).Return(graph.Node{Key: likeKey, Address: addr.Address, Data: likejson, Branches: []string{likeRef}}, true, nil)
		like := timeline.Like{Target: likeKey, Connector: "connector"}
		key, er := p.AddLike(ctx, like, "", "main")
		Expect(er).To(Equal(timeline.ErrCannotRefARef))
		Expect(key).To(Equal(""))

	})

	It("Should get different items by key and count", func() {
		mockCtrl := gomock.NewController(GinkgoT())
		defer mockCtrl.Finish()

		gr := timeline.NewMockGraph(mockCtrl)

		evf, evm := createMockFactoryAndManager(mockCtrl, ns)
		evm.EXPECT().On(timeline.EventTypes.EventReferenced, gomock.Any()).Return(&event.Subscription{})

		tl1, _ := timeline.NewTimeline(ns, addr, gr, evf, logger)

		nodes := []*graph.Node{}
		posts := []timeline.Post{}
		keys := []string{}
		n := 10
		for i := 0; i < n; i++ {
			expectedPost := timeline.Post{
				Base: timeline.Base{Type: timeline.TypePost, Connectors: []string{likeRef}},
				Part: timeline.Part{MimeType: "plain/text", Body: "some text " +
					strconv.Itoa(i)}}
			postjson, _ := json.Marshal(expectedPost)
			postKey := fmt.Sprintf("postKey-%d", i)
			node := &graph.Node{Key: postKey, Address: addr.Address, Data: postjson, Branches: []string{likeRef}}
			nodes = append(nodes, node)
			posts = append(posts, expectedPost)
			keys = append(keys, postKey)
		}

		it := timeline.NewMockIterator(mockCtrl)
		gr.EXPECT().GetIterator(gomock.Any(), "", "", keys[5]).Return(it)

		count := 3
		it.EXPECT().All().DoAndReturn(func() iter.Seq[*graph.Node] {
			index := 2
			return func(yield func(node *graph.Node) bool) {
				for i := index; i >= 0; i-- {
					if !yield(nodes[i]) {
						return
					}
				}
			}
		})

		items, er := tl1.GetFrom(ctx, "", "", keys[5], "", count)

		Expect(er).To(BeNil())
		Expect(len(items)).To(Equal(count))
		l, _ := items[0].Entry.(timeline.Post)
		Expect(l.Part).To(Equal(posts[2].Part))
		m, _ := items[1].Entry.(timeline.Post)
		Expect(m.Part).To(Equal(posts[1].Part))
		l, _ = items[2].Entry.(timeline.Post)
		Expect(l.Part).To(Equal(posts[0].Part))
	})
})

func createMockFactoryAndManager(mockCtrl *gomock.Controller, ns string) (*event.MockManagerFactory, *event.MockManager) {
	evm := event.NewMockManager(mockCtrl)
	evf := event.NewMockManagerFactory(mockCtrl)
	evf.EXPECT().Build(gomock.Any(), gomock.Any(), gomock.Any()).Return(evm, nil)
	return evf, evm
}
