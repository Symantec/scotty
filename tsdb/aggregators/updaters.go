package aggregators

// updaterCreaterType instances are responsible for creating an updaterType
// based on the the number of time slices and the fill policy
// Unspecified fill policies are implicitly the same as None.
type updaterCreaterType map[FillPolicy]func(
	size int, fp FillPolicy) updaterType

var (
	// Use NaN for missing values when fill policy is None, not linear
	// interpolation. NaN is treated as a missing value in aggregations
	kNaN = updaterCreaterType{
		None: newNaNUpdater,
		Zero: newZeroUpdater,
	}
	// use zero for missing values when fill policy is None. Used by the
	// count aggregator.
	kZero = updaterCreaterType{
		None: newZeroUpdater,
		NaN:  newNaNUpdater,
		Null: newNaNUpdater,
	}
)

// Get returns a brand new updaterType based on number of time slices and
// the fill policy.
func (f updaterCreaterType) Get(
	size int, fp FillPolicy) updaterType {
	factory := f[fp]
	if factory == nil {
		factory = f[None]
	}
	return factory(size, fp)
}

// The nanUpdaterType ignores missing values.
type nanUpdaterType struct {
}

func newNaNUpdater(unusedSize int, unusedFp FillPolicy) updaterType {
	return nanUpdaterType{}
}

func (n nanUpdaterType) Update(
	downAgg getByIndexType, aggregators adderType) {
	length := downAgg.Len()
	for i := 0; i < length; i++ {
		downValue, ok := downAgg.Get(i)
		if ok {
			aggregators.Add(i, downValue)
		}
	}
}

// The zeroUpdaterType substitutes zero for missing values.
type zeroUpdaterType struct {
}

func newZeroUpdater(unusedSize int, unusedFp FillPolicy) updaterType {
	return zeroUpdaterType{}
}

func (z zeroUpdaterType) Update(
	downAgg getByIndexType, aggregators adderType) {
	length := downAgg.Len()
	for i := 0; i < length; i++ {
		downValue, ok := downAgg.Get(i)
		if ok {
			aggregators.Add(i, downValue)
		} else {
			aggregators.Add(i, 0.0)
		}
	}
}

// linearInterpolationType does linear interpolation
// Instances of this type do not support simple assignment
type linearInterpolationType struct {
	// Linear interpolated values stored here. The index represents the
	// time slice
	Values []float64
	// Start index. Can't do linear interpolation before first known value
	Start int
	// end index. Can't do linear interpolation after last known value.
	End int
}

// doLinearInterpolation is a convenience function that does linear
// interpolation on g in a single step.
//
// values contains the linear interpolated values. values is always the
// same length as g.
//
// Because linear interpolation cannot be done before the first value or
// after the last value, start is the index in values where linear
// interpolation begins inclusive; end is the index in values where
// linear interpolation ends exclusive.
func doLinearInterpolation(g getByIndexType) (
	values []float64, start, end int) {
	var l linearInterpolationType
	l.Values = make([]float64, g.Len())
	l.Init(g)
	return l.Values, l.Start, l.End
}

// Init initializes this instance with g doing linear interpolation.
// The Values field slice must be the same length as g.
func (l *linearInterpolationType) Init(g getByIndexType) {
	length := g.Len()
	if length != len(l.Values) {
		panic("Lengths don't match")
	}
	l.Start = 0
	l.End = 0
	lastValidIndex := -1
	for i := 0; i < length; i++ {
		value, ok := g.Get(i)
		if ok {
			l.Values[i] = value
			if lastValidIndex >= 0 {
				diff := i - lastValidIndex
				left := diff - 1
				right := 1

				for j := lastValidIndex + 1; j < i; j++ {
					lpart := l.Values[lastValidIndex] * float64(left)
					rpart := l.Values[i] * float64(right)
					l.Values[j] = (lpart + rpart) / float64(diff)
					left--
					right++
				}
			} else {
				l.Start = i
			}
			lastValidIndex = i
			l.End = i + 1
		}
	}
}
