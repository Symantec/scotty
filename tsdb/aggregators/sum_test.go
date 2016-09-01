package aggregators_test

import (
	"github.com/Symantec/scotty/tsdb/aggregators"
	"testing"
)

func TestSum(t *testing.T) {
	tester := newAggregatorTester(aggregators.Sum)
	tester.ExpectNoneForNoValues()
	tester.Expect(5.0, 5.0)
	tester.Expect(10.0, 1.0, 2.0, 3.0, 4.0)
	tester.Expect(3.75, -1.25, 1.5, 3.5)
	tester.Verify(t)
}
