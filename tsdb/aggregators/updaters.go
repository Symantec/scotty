package aggregators

// updaterCreaterType instances are responsible for creating an updaterType
// based on the the number of time slices and the fill policy
type updaterCreaterType map[FillPolicy]func(
	size int, fp FillPolicy) updaterType

var (
	// Linear interpolation for missing values when fill policy is None which
	// is what most aggregators use.
	kLinearInterpolation = updaterCreaterType{
		None: newLinearInterpolationUpdater,
		NaN:  newNaNUpdater,
		Null: newNaNUpdater,
		Zero: newZeroUpdater,
	}
	// use zero for missing values when fill policy is None. Used by the
	// count aggregator.
	kZero = updaterCreaterType{
		None: newZeroUpdater,
		NaN:  newNaNUpdater,
		Null: newNaNUpdater,
	}
	// Used exclusively by the pdiff aggregator.
	kPdiff = updaterCreaterType{
		None: newPdiffUpdater,
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
type linearInterpolationType struct {
	// Linear interpolated values stored here. The index represents the
	// time slice
	Values []float64
	// Start index. Can't do linear interpolation before first known value
	Start int
	// end index. Can't do linear interpolation after last known value.
	End int
}

// Init initializes this instance with g doing linear interpolation.
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

// Performs linear interpolation
type linearInterpolationUpdaterType struct {
	interpolation linearInterpolationType
}

func newLinearInterpolationUpdater(size int, unusedFp FillPolicy) updaterType {
	return &linearInterpolationUpdaterType{
		interpolation: linearInterpolationType{
			Values: make([]float64, size),
		},
	}
}

func (l *linearInterpolationUpdaterType) Update(
	downAgg getByIndexType, aggregators adderType) {
	l.interpolation.Init(downAgg)
	for i := l.interpolation.Start; i < l.interpolation.End; i++ {
		aggregators.Add(i, l.interpolation.Values[i])
	}
}

// For pdiff. Guesses missing data points with linear interpolation.
type pdiffUpdaterType struct {
	interpolation linearInterpolationType
	fillPolicy    FillPolicy
}

func newPdiffUpdater(size int, fp FillPolicy) updaterType {
	return &pdiffUpdaterType{
		interpolation: linearInterpolationType{
			Values: make([]float64, size),
		},
		fillPolicy: fp,
	}
}

func (p *pdiffUpdaterType) Update(
	downAgg getByIndexType, aggregators adderType) {
	p.interpolation.Init(downAgg)
	for i := p.interpolation.Start; i < p.interpolation.End-1; i++ {
		diff := p.interpolation.Values[i+1] - p.interpolation.Values[i]
		if diff >= 0 {
			aggregators.Add(i, diff)
		} else if p.fillPolicy == Zero {
			aggregators.Add(i, 0)
		}
	}
}
