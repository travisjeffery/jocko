package broker_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/travisjeffery/jocko/broker"
	"github.com/travisjeffery/jocko/testutil"
)

func TestBrokerNew(t *testing.T) {
	l := testutil.NewTempDirList()
	defer l.Cleanup()

	t.Run("returns Broker instance", func(t *testing.T) {
		assert.IsType(t, &broker.Broker{}, testutil.NewTestBroker(t, 0, l))
	})

	t.Run("yields instance to BrokerFns", func(t *testing.T) {
		opt0 := func(b *broker.Broker) {
			assert.IsType(t, &broker.Broker{}, b)
		}

		opt1 := func(b *broker.Broker) {
			assert.IsType(t, &broker.Broker{}, b)
		}

		testutil.NewTestBroker(t, 0, l, opt0, opt1)
	})
}
