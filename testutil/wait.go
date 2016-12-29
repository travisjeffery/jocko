package testutil

import "time"

type testFn func() (bool, error)
type errorFn func(error)

func WaitForResult(test testFn, error errorFn) {
	WaitForResultRetries(2000*TestMultiplier(), test, error)
}

func WaitForResultRetries(retries int64, test testFn, error errorFn) {
	for retries > 0 {
		time.Sleep(10 * time.Millisecond)
		retries--

		success, err := test()
		if success {
			return
		}

		if retries == 0 {
			error(err)
		}
	}
}

// TestMultiplier returns a multiplier for retries and waits given environment
// the tests are being run under.
func TestMultiplier() int64 {
	return 1
}
