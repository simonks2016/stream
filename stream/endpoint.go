package stream

import (
	"fmt"
	"strings"
)

type Endpoint struct {
	Kind             EndpointKind   `json:"kind"`
	Name             string         `json:"name"` // topic / channel / stream name
	Meta             map[string]any `json:"meta"` // 可扩展
	EndpointSourceId string         `json:"endpoint_source_id"`
}

type EndpointKind int

const (
	NullEndpointKind EndpointKind = iota
	ConnectorsKind   EndpointKind = iota
	InlineKind
)

func NullEndpoint() Endpoint {

	return Endpoint{
		Kind:             NullEndpointKind,
		Name:             "",
		EndpointSourceId: "",
	}
}

func (e Endpoint) FormattedName() string {

	switch e.Kind {
	case NullEndpointKind:
		return "null"
	case ConnectorsKind:
		connectorName := e.Name
		var subtitle *string = nil

		switch strings.ToLower(e.Name) {
		case "kafka":
			topic, ex := e.Meta["topic"]
			if ex {
				if t1, ok := topic.(string); ok {
					subtitle = &t1
				}
			}
		case "http":
			topic, ex := e.Meta["url"]
			if ex {
				if t1, ok := topic.(string); ok {
					subtitle = &t1
				}
			}
		default:
			break
		}
		return fmt.Sprintf("connectors[%s%s]", connectorName, func() string {
			if subtitle != nil {
				return fmt.Sprintf(",%s", *subtitle)
			}
			return ""
		}())
	case InlineKind:
		return fmt.Sprintf("inline[%s]", e.Name)
	default:
		return fmt.Sprintf("%s", e.Name)
	}

}
