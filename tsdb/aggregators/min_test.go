package aggregators_test

import (
	"github.com/Symantec/scotty/tsdb/aggregators"
	"testing"
)

func TestMinNone(t *testing.T) {
	tester := newAggregatorTester(aggregators.Min, aggregators.None)
	tester.ExpectNoneForNoValues()
	tester.Expect(7.0, 7.0)
	tester.ExpectNoneForNoValues()
	tester.Expect(-3.5, 1.0, 9.0, -3.5, -2.5)
	tester.Expect(2.25, 2.25, 6.0, 3.5)
	tester.Verify(t)
}

func TestMinNaN(t *testing.T) {
	tester := newAggregatorTester(aggregators.Min, aggregators.NaN)
	tester.ExpectNoneForNoValues()
	tester.Expect(7.0, 7.0)
	tester.ExpectNoneForNoValues()
	tester.Expect(-3.5, 1.0, 9.0, -3.5, -2.5)
	tester.Expect(2.25, 2.25, 6.0, 3.5)
	tester.Verify(t)
}

func TestMinNull(t *testing.T) {
	tester := newAggregatorTester(aggregators.Min, aggregators.Null)
	tester.ExpectNoneForNoValues()
	tester.Expect(7.0, 7.0)
	tester.ExpectNoneForNoValues()
	tester.Expect(-3.5, 1.0, 9.0, -3.5, -2.5)
	tester.Expect(2.25, 2.25, 6.0, 3.5)
	tester.Verify(t)
}

func TestMinZero(t *testing.T) {
	tester := newAggregatorTester(aggregators.Min, aggregators.Zero)
	tester.Expect(0.0)
	tester.Expect(7.0, 7.0)
	tester.Expect(0.0)
	tester.Expect(-3.5, 1.0, 9.0, -3.5, -2.5)
	tester.Expect(2.25, 2.25, 6.0, 3.5)
	tester.Verify(t)
}
