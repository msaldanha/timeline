package timeline

import (
	"encoding/json"

	"github.com/msaldanha/setinstone/graph"
)

const (
	ConnectorMain       = "main"
	ConnectorLike       = "like"
	ConnectorComment    = "comment"
	TypePost            = "Post"
	TypeReference       = "Reference"
	TypeLike            = "Like"
	TypeReceivedLike    = "ReceivedLike"
	TypeComment         = "Comment"
	TypeReceivedComment = "ReceivedComment"
	TypeReply           = "Reply"
	TypeRepost          = "Repost"
)

// Base is the common structure for timeline items.
// It defines the type of the item and the connectors it can use.
type Base struct {
	Type       string   `json:"type,omitempty"`
	Connectors []string `json:"connectors,omitempty"`
}

// Part represents a piece of content with metadata.
// It includes information about the content type, encoding, title, and body.
type Part struct {
	MimeType string `json:"mimeType,omitempty"`
	Encoding string `json:"encoding,omitempty"`
	Title    string `json:"title,omitempty"`
	Body     string `json:"body,omitempty"`
}

// PostPart extends Part with sequence and name information.
// It is used for links and attachments in a Post.
type PostPart struct {
	Seq  int    `json:"seq,omitempty"`
	Name string `json:"name,omitempty"`
	Part
}

// Post represents a content item in a timeline.
// It extends Base and Part, and can include links and attachments.
type Post struct {
	Base
	Part
	Links       []PostPart `json:"links,omitempty"`
	Attachments []PostPart `json:"attachments,omitempty"`
}

type Like struct {
	Base
	Connector string `json:"connector,omitempty"`
	Target    string `json:"target,omitempty"`
}

type ReceivedLike struct {
	Base
	Connector string `json:"connector,omitempty"`
	Origin    string `json:"origin,omitempty"`
	Target    string `json:"target,omitempty"`
}

type Comment struct {
	Post
	Connector string `json:"connector,omitempty"`
	Target    string `json:"target,omitempty"`
}

type ReceivedComment struct {
	Base
	Connector string `json:"connector,omitempty"`
	Origin    string `json:"origin,omitempty"`
	Target    string `json:"target,omitempty"`
}

// Item represents an entry in a timeline, which can be either a Post or a Reference.
// It extends graph.Node and includes either a Post or a Reference.
type Item struct {
	Key       string   `json:"key,omitempty"`
	Seq       int32    `json:"seq,omitempty"`
	Timestamp string   `json:"timestamp,omitempty"`
	Address   string   `json:"address,omitempty"`
	Branches  []string `json:"branches,omitempty"`
	Entry     any      `json:"entry,omitempty"`
}

// NewItemFromGraphNode creates an Item from a graph.Node.
// It unmarshals the node data to determine if it's a Post or a Reference.
// Returns the created Item and an error if unmarshaling fails or if the type is unknown.
func NewItemFromGraphNode(v graph.Node) (Item, error) {
	base := Base{}
	er := json.Unmarshal(v.Data, &base)
	if er != nil {
		return Item{}, er
	}

	item := Item{
		Key:       v.Key,
		Seq:       v.Seq,
		Timestamp: v.Timestamp,
		Address:   v.Address,
		Branches:  v.Branches,
	}

	switch base.Type {
	case TypePost:
		item.Entry, er = unmarshal[Post](v.Data)
	case TypeLike:
		item.Entry, er = unmarshal[Like](v.Data)
	case TypeReceivedLike:
		item.Entry, er = unmarshal[ReceivedLike](v.Data)
	case TypeComment:
		item.Entry, er = unmarshal[Comment](v.Data)
	case TypeReceivedComment:
		item.Entry, er = unmarshal[ReceivedComment](v.Data)
	default:
		return Item{}, ErrUnknownType
	}

	if er != nil {
		return Item{}, er
	}

	return item, nil
}

func unmarshal[T any](data []byte) (T, error) {
	var v T
	err := json.Unmarshal(data, &v)
	return v, err
}
