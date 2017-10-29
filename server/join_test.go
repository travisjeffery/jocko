package server

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/travisjeffery/jocko"
	"github.com/travisjeffery/jocko/protocol"
	"github.com/travisjeffery/jocko/testutil/mock"
	"github.com/travisjeffery/simplelog"
)

func TestJoin(t *testing.T) {
	b := &mock.Broker{
		JoinFn: func(addr ...string) protocol.Error {
			return protocol.ErrNone
		},
		RunFn: func(context.Context, <-chan jocko.Request, chan<- jocko.Response) {},
	}
	logger := simplelog.New(os.Stdout, simplelog.DEBUG, "server/test")

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
		if !b.JoinInvoked {
			t.Error("expected join invoked")
		}
	})

	b.JoinInvoked = false

	t.Run("bad join request", func(tt *testing.T) {
		req := httptest.NewRequest("POST", "http://localhost:9094/join", nil)
		w := httptest.NewRecorder()

		srv.handleJoin(w, req)

		res := w.Result()
		if res.StatusCode != http.StatusBadRequest {
			t.Errorf("expected join to %d, got %d", http.StatusBadRequest, res.StatusCode)
		}
		if b.JoinInvoked {
			t.Error("expected join not invoked")
		}
	})
}
