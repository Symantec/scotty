package aggregators_test

import (
	"github.com/Symantec/scotty/tsdb/aggregators"
	"testing"
)

func TestSumNone(t *testing.T) {
	tester := newAggregatorTester(aggregators.Sum, aggregators.None)
	tester.ExpectNoneForNoValues()
	tester.Expect(5.0, 5.0)
	tester.ExpectNoneForNoValues()
	tester.Expect(10.0, 1.0, 2.0, 3.0, 4.0)
	tester.Expect(3.75, -1.25, 1.5, 3.5)
	tester.Verify(t)
}

func TestSumNaN(t *testing.T) {
	tester := newAggregatorTester(aggregators.Sum, aggregators.NaN)
	tester.ExpectNoneForNoValues()
	tester.Expect(5.0, 5.0)
	tester.ExpectNoneForNoValues()
	tester.Expect(10.0, 1.0, 2.0, 3.0, 4.0)
	tester.Expect(3.75, -1.25, 1.5, 3.5)
	tester.Verify(t)
}

func TestSumNull(t *testing.T) {
	tester := newAggregatorTester(aggregators.Sum, aggregators.Null)
	tester.ExpectNoneForNoValues()
	tester.Expect(5.0, 5.0)
	tester.ExpectNoneForNoValues()
	tester.Expect(10.0, 1.0, 2.0, 3.0, 4.0)
	tester.Expect(3.75, -1.25, 1.5, 3.5)
	tester.Verify(t)
}

func TestSumZero(t *testing.T) {
	tester := newAggregatorTester(aggregators.Sum, aggregators.Zero)
	tester.Expect(0.0)
	tester.Expect(5.0, 5.0)
	tester.Expect(0.0)
	tester.Expect(10.0, 1.0, 2.0, 3.0, 4.0)
	tester.Expect(3.75, -1.25, 1.5, 3.5)
	tester.Verify(t)
}
