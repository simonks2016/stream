package httpconnector

import "net/http"

type HttpSchema struct {
	Header http.Header
	Body   []byte
	Cookie *http.Cookie
}

type HttpOrigin struct {
	Url    string `json:"url"`
	Method string `json:"method"`
}
type HttpResponse struct {
	schema    HttpSchema
	RequestId string
	origin    HttpOrigin
	err       error
}
