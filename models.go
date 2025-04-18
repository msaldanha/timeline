package timeline

import (
	"encoding/json"

	"github.com/msaldanha/setinstone/graph"
)

const (
	TypePost      = "Post"
	TypeReference = "Reference"
)

type Base struct {
	Type       string   `json:"type,omitempty"`
	Connectors []string `json:"connectors,omitempty"`
}

type Part struct {
	MimeType string `json:"mimeType,omitempty"`
	Encoding string `json:"encoding,omitempty"`
	Title    string `json:"title,omitempty"`
	Body     string `json:"body,omitempty"`
}

type PostPart struct {
	Seq  int    `json:"seq,omitempty"`
	Name string `json:"name,omitempty"`
	Part
}

type Post struct {
	Base
	Part
	Links       []PostPart `json:"links,omitempty"`
	Attachments []PostPart `json:"attachments,omitempty"`
}

type Reference struct {
	Base
	Connector string `json:"connector,omitempty"`
	Target    string `json:"target,omitempty"`
}

type Item struct {
	graph.Node
	Post      *Post      `json:"post,omitempty"`
	Reference *Reference `json:"reference,omitempty"`
}

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
