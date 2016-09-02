package aggregators_test

import (
	"github.com/Symantec/scotty/tsdb/aggregators"
	"testing"
)

func TestCount(t *testing.T) {
	tester := newAggregatorTester(aggregators.Count)
	tester.Expect(0.0)
	tester.Expect(1.0, 5.0)
	tester.Expect(4.0, 1.0, 2.0, 3.0, 4.0)
	tester.Expect(3.0, -1.25, 1.5, 3.5)
	tester.Verify(t)
}
