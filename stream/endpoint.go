package stream

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
