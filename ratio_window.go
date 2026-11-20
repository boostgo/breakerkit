package breakerkit

import "time"

// ratioBuckets is the number of buckets of the WithFailureRatio sliding window.
const ratioBuckets = 10

type ratioBucket struct {
	successes uint64
	failures  uint64
}

// ratioWindow is a bucketed sliding window of outcomes. It is not goroutine-safe: it is used
// only under the breaker mutex. The window moves with the passed time, without tickers.
type ratioWindow struct {
	ratio       float64
	minRequests int
	bucketDur   time.Duration
	buckets     [ratioBuckets]ratioBucket
	head        int
	headStart   time.Time
}

func newRatioWindow(ratio float64, minRequests int, window time.Duration) *ratioWindow {
	bucketDur := window / ratioBuckets
	if bucketDur <= 0 {
		bucketDur = 1
	}
	return &ratioWindow{ratio: ratio, minRequests: minRequests, bucketDur: bucketDur}
}

// advance drops the buckets that have left the window by now.
func (w *ratioWindow) advance(now time.Time) {
	if w.headStart.IsZero() {
		w.headStart = now
		return
	}
	elapsed := now.Sub(w.headStart)
	if elapsed < w.bucketDur {
		return
	}
	steps := elapsed / w.bucketDur
	if steps >= ratioBuckets {
		w.buckets = [ratioBuckets]ratioBucket{}
		w.head = 0
		w.headStart = now
		return
	}
	for range int(steps) {
		w.head = (w.head + 1) % ratioBuckets
		w.buckets[w.head] = ratioBucket{}
	}
	w.headStart = w.headStart.Add(steps * w.bucketDur)
}

func (w *ratioWindow) record(now time.Time, success bool) {
	w.advance(now)
	if success {
		w.buckets[w.head].successes++
	} else {
		w.buckets[w.head].failures++
	}
}

func (w *ratioWindow) totals(now time.Time) (successes, failures uint64) {
	w.advance(now)
	for _, b := range w.buckets {
		successes += b.successes
		failures += b.failures
	}
	return successes, failures
}

func (w *ratioWindow) exceeded(successes, failures uint64) bool {
	requests := successes + failures
	if requests == 0 || requests < toUint64(w.minRequests) {
		return false
	}
	return float64(failures)/float64(requests) >= w.ratio
}

func (w *ratioWindow) reset() {
	w.buckets = [ratioBuckets]ratioBucket{}
	w.head = 0
	w.headStart = time.Time{}
}
