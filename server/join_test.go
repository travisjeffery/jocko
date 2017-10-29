package server

import (
	"bytes"
	"context"
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
	srv := New("localhost:9092", b, "localhost:9093", logger)
	srv.Start(context.Background())
	defer srv.Close()
	buf := bytes.NewBufferString(`{}`)
	req := httptest.NewRequest("POST", "http://localhost:9094/join", buf)
	w := httptest.NewRecorder()
	srv.handleJoin(w, req)
	res := w.Result()
	if res.StatusCode != 200 {
		t.Errorf("expected join to 200, got %d", res.StatusCode)
	}
	if !b.JoinInvoked {
		t.Error("expected join invoked")
	}
}
