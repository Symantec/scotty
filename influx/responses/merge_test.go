package responses

import (
	"encoding/json"
	"github.com/influxdata/influxdb/models"
	. "github.com/smartystreets/goconvey/convey"
	"strconv"
	"testing"
)

func newPoint1(x int64) []interface{} {
	return []interface{}{
		json.Number(strconv.FormatInt(x, 10)), nil}
}

func newPoint2(x int64, val float64) []interface{} {
	return []interface{}{
		json.Number(strconv.FormatInt(x, 10)),
		json.Number(strconv.FormatFloat(val, 'g', -1, 64))}
}

func TestQuotient(t *testing.T) {
	Convey("dividing", t, func() {
		s1 := [][]interface{}{
			newPoint2(13000, 92.6),
			newPoint2(13100, 38.4),
			newPoint2(13200, 46.5),
			newPoint2(13300, 66.0),
		}
		s2 := [][]interface{}{
			newPoint2(13100, 0.0),
			newPoint2(13200, 3.0),
			newPoint2(13300, 4.0),
			newPoint2(13400, 5.3),
		}
		quotient, err := piecewiseDivide(s1, s2)
		So(err, ShouldBeNil)
		So(quotient, ShouldResemble, [][]interface{}{
			newPoint1(13100),
			newPoint2(13200, 15.5),
			newPoint2(13300, 16.5),
		})
	})
}

func TestSum(t *testing.T) {
	Convey("Summing serires", t, func() {
		s1 := [][]interface{}{
			newPoint2(13000, 92.4),
			newPoint2(13100, 38.9),
			newPoint2(13200, 45.5),
			newPoint2(13300, 62.1),
		}
		s2 := [][]interface{}{
			newPoint2(13200, 10.0),
			newPoint2(13300, 20.0),
			newPoint2(13400, 5.3),
		}
		s3 := [][]interface{}{
			newPoint2(13700, 8.9),
		}
		s4 := [][]interface{}{}
		sum, err := sumTogether2(s1, s2)
		So(err, ShouldBeNil)
		sum, err = sumTogether2(sum, s3)
		So(err, ShouldBeNil)
		sum, err = sumTogether2(sum, s4)
		So(err, ShouldBeNil)
		So(sum, ShouldResemble, [][]interface{}{
			newPoint2(13000, 92.4),
			newPoint2(13100, 38.9),
			newPoint2(13200, 55.5),
			newPoint2(13300, 82.1),
			newPoint2(13400, 5.3),
			newPoint2(13700, 8.9),
		})
	})
}

func TestDivideRows(t *testing.T) {
	Convey("Dividing rows", t, func() {
		lhs := []models.Row{
			{
				Name:    "loadMetric",
				Tags:    map[string]string{"appname": "subd"},
				Columns: []string{"time", "sum"},
				Values: [][]interface{}{
					newPoint2(15000, 400.0),
					newPoint2(15010, 500.0),
					newPoint2(15020, 600.0),
				},
			},
			{
				Name:    "loadMetric",
				Tags:    map[string]string{"appname": "imageserver"},
				Columns: []string{"time", "sum"},
				Values: [][]interface{}{
					newPoint2(15000, 100.0),
					newPoint2(15010, 200.0),
					newPoint2(15020, 300.0),
				},
			},
			{
				Name:    "loadMetric",
				Tags:    map[string]string{"appname": "health"},
				Columns: []string{"time", "sum"},
				Values: [][]interface{}{
					newPoint2(15000, 900.0),
					newPoint2(15010, 800.0),
					newPoint2(15020, 700.0),
				},
			},
		}
		rhs := []models.Row{
			{
				Name:    "loadMetric",
				Tags:    map[string]string{"appname": "subd"},
				Columns: []string{"time", "count"},
				Values: [][]interface{}{
					newPoint2(15000, 1.0),
					newPoint2(15010, 2.0),
					newPoint2(15020, 4.0),
				},
			},
			{
				Name:    "loadMetric",
				Tags:    map[string]string{"appname": "imageserver"},
				Columns: []string{"time", "count"},
				Values: [][]interface{}{
					newPoint2(15000, 2.0),
					newPoint2(15010, 4.0),
					newPoint2(15020, 8.0),
				},
			},
			{
				Name:    "loadMetric",
				Tags:    map[string]string{"appname": "viz"},
				Columns: []string{"time", "count"},
				Values: [][]interface{}{
					newPoint2(15000, 12.0),
					newPoint2(15010, 15.0),
					newPoint2(15020, 18.0),
				},
			},
		}
		quotient, err := DivideRows(lhs, rhs, []string{"time", "mean"})
		So(err, ShouldBeNil)
		So(quotient, ShouldResemble, []models.Row{
			{
				Name:    "loadMetric",
				Tags:    map[string]string{"appname": "imageserver"},
				Columns: []string{"time", "mean"},
				Values: [][]interface{}{
					newPoint2(15000, 50.0),
					newPoint2(15010, 50.0),
					newPoint2(15020, 37.5),
				},
			},
			{
				Name:    "loadMetric",
				Tags:    map[string]string{"appname": "subd"},
				Columns: []string{"time", "mean"},
				Values: [][]interface{}{
					newPoint2(15000, 400.0),
					newPoint2(15010, 250.0),
					newPoint2(15020, 150.0),
				},
			},
		})
	})
}

func TestSumRowsTogether(t *testing.T) {
	Convey("Summing like rows", t, func() {
		rowGroup1 := []models.Row{
			{
				Name:    "metric",
				Tags:    map[string]string{"appname": "subd"},
				Columns: []string{"time", "sum"},
				Values: [][]interface{}{
					newPoint2(15000, 17.0),
					newPoint2(15010, 20.0),
					newPoint2(15020, 23.0),
				},
			},
			{
				Name:    "metric",
				Tags:    map[string]string{"appname": "imageserver"},
				Columns: []string{"time", "sum"},
				Values: [][]interface{}{
					newPoint2(15000, 27.0),
					newPoint2(15010, 30.0),
					newPoint2(15020, 33.0),
				},
			},
		}
		rowGroup2 := []models.Row{
			{
				Name:    "metric",
				Tags:    map[string]string{"appname": "subd"},
				Columns: []string{"time", "sum"},
				Values: [][]interface{}{
					newPoint2(15000, 10.0),
					newPoint2(15010, 9.0),
					newPoint2(15020, 8.0),
				},
			},
			{
				Name:    "metric",
				Tags:    map[string]string{"appname": "imageserver"},
				Columns: []string{"time", "sum"},
				Values: [][]interface{}{
					newPoint2(15000, 20.0),
					newPoint2(15010, 19.0),
					newPoint2(15020, 18.0),
				},
			},
		}
		summedRows, err := SumRowsTogether(rowGroup1, rowGroup2)
		So(err, ShouldBeNil)
		So(summedRows, ShouldResemble, []models.Row{
			{
				Name:    "metric",
				Tags:    map[string]string{"appname": "imageserver"},
				Columns: []string{"time", "sum"},
				Values: [][]interface{}{
					newPoint2(15000, 47.0),
					newPoint2(15010, 49.0),
					newPoint2(15020, 51.0),
				},
			},
			{
				Name:    "metric",
				Tags:    map[string]string{"appname": "subd"},
				Columns: []string{"time", "sum"},
				Values: [][]interface{}{
					newPoint2(15000, 27.0),
					newPoint2(15010, 29.0),
					newPoint2(15020, 31.0),
				},
			},
		})
	})
	Convey("Summing unlike rows", t, func() {
		rowGroup1 := []models.Row{
			{
				Name:    "metric",
				Tags:    map[string]string{"region": "us-east-1"},
				Columns: []string{"time", "sum"},
				Values: [][]interface{}{
					newPoint2(15000, 17.0),
					newPoint2(15010, 20.0),
					newPoint2(15020, 23.0),
				},
			},
		}
		rowGroup2 := []models.Row{
			{
				Name:    "metric",
				Tags:    map[string]string{"region": "us-west-2"},
				Columns: []string{"time", "sum"},
				Values: [][]interface{}{
					newPoint2(15000, 10.0),
					newPoint2(15010, 9.0),
					newPoint2(15020, 8.0),
				},
			},
		}
		summedRows, err := SumRowsTogether(rowGroup1, rowGroup2)
		So(err, ShouldBeNil)
		So(summedRows, ShouldResemble, []models.Row{
			{
				Name:    "metric",
				Tags:    map[string]string{"region": "us-east-1"},
				Columns: []string{"time", "sum"},
				Values: [][]interface{}{
					newPoint2(15000, 17.0),
					newPoint2(15010, 20.0),
					newPoint2(15020, 23.0),
				},
			},
			{
				Name:    "metric",
				Tags:    map[string]string{"region": "us-west-2"},
				Columns: []string{"time", "sum"},
				Values: [][]interface{}{
					newPoint2(15000, 10.0),
					newPoint2(15010, 9.0),
					newPoint2(15020, 8.0),
				},
			},
		})
	})
}
