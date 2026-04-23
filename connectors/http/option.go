package httpconnector

import (
	"time"

	"github.com/simonks2016/stream/stream"
)

type Option func(connector *HttpConnector)

func WithMaxIdleConn(maxIdleConns int) Option {
	return func(connector *HttpConnector) {
		connector.MaxIdleConns = maxIdleConns
	}
}
func WithIdleConnTimeout(timeout time.Duration) Option {
	return func(connector *HttpConnector) {
		connector.IdleConnTimeout = timeout
	}
}
func WithTLSHandshakeTimeout(timeout time.Duration) Option {
	return func(connector *HttpConnector) {
		connector.TLSHandshakeTimeout = timeout
	}
}
func WithRequestTimeout(timeout time.Duration) Option {
	return func(connector *HttpConnector) {
		connector.RequestTimeout = timeout
	}
}
func WithError(ep stream.Endpoint) Option {
	return func(connector *HttpConnector) {
		connector.DefaultErrorEndpoint = ep
	}
}
