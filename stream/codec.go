package stream

type Codec interface {
	Encode(any) ([]byte, error)
	Decode([]byte, any) error
	Name() string
}
