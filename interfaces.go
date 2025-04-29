package timeline

import (
	"context"
	"iter"

	"github.com/msaldanha/setinstone/address"
	"github.com/msaldanha/setinstone/graph"
)

//go:generate mockgen -source=interfaces.go -destination=graph_mock.go -package=timeline Graph,Iterator

// Graph defines the interface for a graph data structure used by the timeline.
// It provides methods for storing, retrieving, and iterating over nodes in the graph.
type Graph interface {
	GetName() string
	GetMetaData() string
	Get(ctx context.Context, key string) (graph.Node, bool, error)
	Append(ctx context.Context, keyRoot string, node graph.NodeData) (graph.Node, error)
	GetIterator(ctx context.Context, keyRoot, branch string, from string) graph.Iterator
	GetAddress(ctx context.Context) *address.Address
	Manage(addr *address.Address) error
}

// Iterator defines the interface for iterating over nodes in a graph.
// It provides methods for navigating through the nodes and retrieving them.
type Iterator interface {
	Last() (*graph.Node, error)
	Prev() (*graph.Node, error)
	All() iter.Seq[*graph.Node]
}
