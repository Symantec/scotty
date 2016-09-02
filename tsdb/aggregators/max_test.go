package aggregators_test

import (
	"github.com/Symantec/scotty/tsdb/aggregators"
	"testing"
)

func TestMax(t *testing.T) {
	tester := newAggregatorTester(aggregators.Max)
	tester.ExpectNoneForNoValues()
	tester.Expect(5.0, 5.0)
	tester.Expect(3.0, 1.0, 2.0, 3.0, -1.0)
	tester.Expect(19.5, 19.5, 4.25, 3.5)
	tester.Verify(t)
}
