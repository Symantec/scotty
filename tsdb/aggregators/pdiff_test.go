package aggregators_test

import (
	"github.com/Symantec/scotty/tsdb/aggregators"
	"testing"
)

func TestPdiffNone(t *testing.T) {
	tester := newAggregatorTester(aggregators.Pdiff, aggregators.None)
	tester.ExpectNoneForNoValues()
	tester.Expect(2.0, 3.0, 5.0)
	tester.Expect(2.0)
	tester.Expect(4.0, 7.0, 9.0)
	tester.Expect(2.5, 11.0, 17.0, 19.0, 20.0)
	tester.Expect(2.5)
	tester.Expect(2.5)
	tester.Expect(2.5)
	tester.ExpectNone(21.0)
	tester.Expect(15.0, 5.0, 6.0, 7.0)
	tester.ExpectNone(20.0)
	tester.Verify(t)
}

func TestPdiffNaN(t *testing.T) {
	tester := newAggregatorTester(aggregators.Pdiff, aggregators.NaN)
	tester.ExpectNoneForNoValues()
	tester.Expect(2.0, 3.0, 5.0)
	tester.Expect(2.0)
	tester.Expect(4.0, 7.0, 9.0)
	tester.Expect(2.5, 11.0, 17.0, 19.0, 20.0)
	tester.Expect(2.5)
	tester.Expect(2.5)
	tester.Expect(2.5)
	tester.ExpectNone(21.0)
	tester.Expect(15.0, 5.0, 6.0, 7.0)
	tester.ExpectNone(20.0)
	tester.Verify(t)
}

func TestPdiffNull(t *testing.T) {
	tester := newAggregatorTester(aggregators.Pdiff, aggregators.Null)
	tester.ExpectNoneForNoValues()
	tester.Expect(2.0, 3.0, 5.0)
	tester.Expect(2.0)
	tester.Expect(4.0, 7.0, 9.0)
	tester.Expect(2.5, 11.0, 17.0, 19.0, 20.0)
	tester.Expect(2.5)
	tester.Expect(2.5)
	tester.Expect(2.5)
	tester.ExpectNone(21.0)
	tester.Expect(15.0, 5.0, 6.0, 7.0)
	tester.ExpectNone(20.0)
	tester.Verify(t)
}

func TestPdiffZero(t *testing.T) {
	tester := newAggregatorTester(aggregators.Pdiff, aggregators.Zero)
	tester.ExpectNoneForNoValues()
	tester.Expect(2.0, 3.0, 5.0)
	tester.Expect(2.0)
	tester.Expect(4.0, 7.0, 9.0)
	tester.Expect(2.5, 11.0, 17.0, 19.0, 20.0)
	tester.Expect(2.5)
	tester.Expect(2.5)
	tester.Expect(2.5)
	tester.Expect(0.0, 21.0)
	tester.Expect(0.0)
	tester.Expect(15.0, 5.0, 6.0, 7.0)
	tester.ExpectNone(20.0)
	tester.Verify(t)
}
