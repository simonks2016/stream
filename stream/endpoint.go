package stream

type Endpoint struct {
	Kind             EndpointKind   `json:"kind"`
	Name             string         `json:"name"` // topic / channel / stream name
	Meta             map[string]any `json:"meta"` // 可扩展
	EndpointSourceId string         `json:"endpoint_source_id"`
}

type EndpointKind int

const (
	ConnectorsKind EndpointKind = iota
	InlineKind
)
