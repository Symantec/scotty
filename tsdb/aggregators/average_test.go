package aggregators_test

import (
	"github.com/Symantec/scotty/tsdb/aggregators"
	"testing"
)

func TestAvg(t *testing.T) {
	tester := newAggregatorTester(aggregators.Avg)
	tester.ExpectNoneForNoValues()
	tester.Expect(5.0, 5.0)
	tester.Expect(2.5, 1.0, 2.0, 3.0, 4.0)
	tester.Expect(1.25, -1.25, 1.5, 3.5)
	tester.Verify(t)
}
