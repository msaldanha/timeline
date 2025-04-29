package timeline

import (
	"encoding/json"

	"github.com/msaldanha/setinstone/graph"
)

const (
	TypePost      = "Post"
	TypeReference = "Reference"
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

// Reference represents a link to a Post in another timeline.
// It extends Base and includes the connector to use and the target Post key.
type Reference struct {
	Base
	Connector string `json:"connector,omitempty"`
	Target    string `json:"target,omitempty"`
}

// Item represents an entry in a timeline, which can be either a Post or a Reference.
// It extends graph.Node and includes either a Post or a Reference.
type Item struct {
	graph.Node
	Post      *Post      `json:"post,omitempty"`
	Reference *Reference `json:"reference,omitempty"`
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
		Node: v,
	}

	switch base.Type {
	case TypeReference:
		ri := Reference{}
		er = json.Unmarshal(v.Data, &ri)
		item.Reference = &ri
	case TypePost:
		p := Post{}
		er = json.Unmarshal(v.Data, &p)
		item.Post = &p
	default:
		er = ErrUnknownType
	}

	if er != nil {
		return Item{}, er
	}

	return item, nil
}
