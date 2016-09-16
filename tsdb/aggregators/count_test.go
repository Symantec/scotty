package aggregators_test

import (
	"github.com/Symantec/scotty/tsdb/aggregators"
	"testing"
)

func TestCountNone(t *testing.T) {
	tester := newAggregatorTester(aggregators.Count, aggregators.None)
	tester.Expect(0.0)
	tester.Expect(1.0, 5.0)
	tester.Expect(0.0)
	tester.Expect(0.0)
	tester.Expect(4.0, 1.0, 2.0, 3.0, 4.0)
	tester.Expect(3.0, -1.25, 1.5, 3.5)
	tester.Verify(t)
}

func TestCountNaN(t *testing.T) {
	tester := newAggregatorTester(aggregators.Count, aggregators.NaN)
	tester.Expect(0.0)
	tester.Expect(1.0, 5.0)
	tester.Expect(0.0)
	tester.Expect(0.0)
	tester.Expect(4.0, 1.0, 2.0, 3.0, 4.0)
	tester.Expect(3.0, -1.25, 1.5, 3.5)
	tester.Verify(t)
}

func TestCountNull(t *testing.T) {
	tester := newAggregatorTester(aggregators.Count, aggregators.Null)
	tester.Expect(0.0)
	tester.Expect(1.0, 5.0)
	tester.Expect(0.0)
	tester.Expect(0.0)
	tester.Expect(4.0, 1.0, 2.0, 3.0, 4.0)
	tester.Expect(3.0, -1.25, 1.5, 3.5)
	tester.Verify(t)
}

func TestCountZero(t *testing.T) {
	tester := newAggregatorTester(aggregators.Count, aggregators.Zero)
	tester.Expect(0.0)
	tester.Expect(1.0, 5.0)
	tester.Expect(0.0)
	tester.Expect(0.0)
	tester.Expect(4.0, 1.0, 2.0, 3.0, 4.0)
	tester.Expect(3.0, -1.25, 1.5, 3.5)
	tester.Verify(t)
}
