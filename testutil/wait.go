package testutil

import (
	"errors"
	"time"
)

type testFn func() (bool, error)
type errorFn func(error)

func WaitForResult(test testFn, error errorFn) {
	waitForResultRetries(2000*TestMultiplier(), test, error)
}

func waitForResultRetries(retries int64, test testFn, error errorFn) {
	for retries > 0 {
		time.Sleep(10 * time.Millisecond)
		retries--

		success, err := test()
		if success {
			return
		}

		if retries == 0 {
			if err != nil {
				error(err)
			} else {
				error(errors.New("max number of retries exceeded"))
			}
		}
	}
}

// TestMultiplier returns a multiplier for retries and waits given environment
// the tests are being run under.
func TestMultiplier() int64 {
	return 1
}
