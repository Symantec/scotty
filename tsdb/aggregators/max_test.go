package aggregators_test

import (
	"github.com/Symantec/scotty/tsdb/aggregators"
	"testing"
)

func TestMaxNone(t *testing.T) {
	tester := newAggregatorTester(aggregators.Max, aggregators.None)
	tester.ExpectNoneForNoValues()
	tester.Expect(5.0, 5.0)
	tester.Expect(4.25)
	tester.Expect(3.5)
	tester.Expect(2.75)
	tester.Expect(2.0, 1.0, 2.0, 1.75, -1.0)
	tester.Expect(19.5, 19.5, 4.25, 3.5)
	tester.Verify(t)
}

func TestMaxNaN(t *testing.T) {
	tester := newAggregatorTester(aggregators.Max, aggregators.NaN)
	tester.ExpectNoneForNoValues()
	tester.Expect(5.0, 5.0)
	tester.ExpectNoneForNoValues()
	tester.ExpectNoneForNoValues()
	tester.ExpectNoneForNoValues()
	tester.Expect(2.0, 1.0, 2.0, 1.75, -1.0)
	tester.Expect(19.5, 19.5, 4.25, 3.5)
	tester.Verify(t)
}

func TestMaxNull(t *testing.T) {
	tester := newAggregatorTester(aggregators.Max, aggregators.Null)
	tester.ExpectNoneForNoValues()
	tester.Expect(5.0, 5.0)
	tester.ExpectNoneForNoValues()
	tester.ExpectNoneForNoValues()
	tester.ExpectNoneForNoValues()
	tester.Expect(2.0, 1.0, 2.0, 1.75, -1.0)
	tester.Expect(19.5, 19.5, 4.25, 3.5)
	tester.Verify(t)
}

func TestMaxZero(t *testing.T) {
	tester := newAggregatorTester(aggregators.Max, aggregators.Zero)
	tester.Expect(0.0)
	tester.Expect(5.0, 5.0)
	tester.Expect(0.0)
	tester.Expect(0.0)
	tester.Expect(0.0)
	tester.Expect(2.0, 1.0, 2.0, 1.75, -1.0)
	tester.Expect(19.5, 19.5, 4.25, 3.5)
	tester.Verify(t)
}
