package server

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/travisjeffery/jocko"
	"github.com/travisjeffery/jocko/mock"
	"github.com/travisjeffery/jocko/protocol"
	"github.com/travisjeffery/jocko/zap"
)

func TestJoin(t *testing.T) {
	b := &mock.Broker{
		JoinFunc: func(addr ...string) protocol.Error {
			return protocol.ErrNone
		},
		RunFunc: func(context.Context, <-chan jocko.Request, chan<- jocko.Response) {},
	}
	logger := zap.New()
	srv := New("localhost:9092", b, "localhost:9093", mock.NewMetrics(), logger)
	srv.Start(context.Background())
	defer srv.Close()

	t.Run("ok", func(tt *testing.T) {
		buf := bytes.NewBufferString(`{}`)
		req := httptest.NewRequest("POST", "http://localhost:9094/join", buf)
		w := httptest.NewRecorder()

		srv.handleJoin(w, req)

		res := w.Result()
		if res.StatusCode != http.StatusOK {
			t.Errorf("expected join to %d, got %d", http.StatusOK, res.StatusCode)
		}
		if !b.JoinCalled() {
			t.Error("expected join invoked")
		}
	})

	b.Reset()

	t.Run("bad join request", func(tt *testing.T) {
		req := httptest.NewRequest("POST", "http://localhost:9094/join", nil)
		w := httptest.NewRecorder()

		srv.handleJoin(w, req)

		res := w.Result()
		if res.StatusCode != http.StatusBadRequest {
			t.Errorf("expected join to %d, got %d", http.StatusBadRequest, res.StatusCode)
		}
		if b.JoinCalled() {
			t.Error("expected join not invoked")
		}
	})
}
