package aggregators_test

import (
	"github.com/Symantec/scotty/tsdb/aggregators"
	"testing"
)

func TestAvgNone(t *testing.T) {
	tester := newAggregatorTester(aggregators.Avg, aggregators.None)
	tester.ExpectNoneForNoValues()
	tester.Expect(5.0, 5.0)
	tester.Expect(4.5)
	tester.Expect(4.0)
	tester.Expect(3.5, 1.0, 2.0, 3.0, 8.0)
	tester.Expect(1.25, -1.25, 1.5, 3.5)
	tester.Verify(t)
}

func TestAvgNaN(t *testing.T) {
	tester := newAggregatorTester(aggregators.Avg, aggregators.NaN)
	tester.ExpectNoneForNoValues()
	tester.Expect(5.0, 5.0)
	tester.ExpectNoneForNoValues()
	tester.ExpectNoneForNoValues()
	tester.Expect(2.5, 1.0, 2.0, 3.0, 4.0)
	tester.Expect(1.25, -1.25, 1.5, 3.5)
	tester.Verify(t)
}

func TestAvgNull(t *testing.T) {
	tester := newAggregatorTester(aggregators.Avg, aggregators.Null)
	tester.ExpectNoneForNoValues()
	tester.Expect(5.0, 5.0)
	tester.ExpectNoneForNoValues()
	tester.ExpectNoneForNoValues()
	tester.Expect(2.5, 1.0, 2.0, 3.0, 4.0)
	tester.Expect(1.25, -1.25, 1.5, 3.5)
	tester.Verify(t)
}

func TestAvgZero(t *testing.T) {
	tester := newAggregatorTester(aggregators.Avg, aggregators.Zero)
	tester.Expect(0.0)
	tester.Expect(5.0, 5.0)
	tester.Expect(0.0)
	tester.Expect(0.0)
	tester.Expect(2.5, 1.0, 2.0, 3.0, 4.0)
	tester.Expect(1.25, -1.25, 1.5, 3.5)
	tester.Verify(t)
}
