package responses_test

import (
	"encoding/json"
	"github.com/Symantec/scotty/influx/responses"
	"github.com/Symantec/scotty/tsdb"
	"github.com/Symantec/scotty/tsdbjson"
	"github.com/influxdata/influxdb/client/v2"
	"github.com/influxdata/influxdb/models"
	. "github.com/smartystreets/goconvey/convey"
	"strconv"
	"testing"
)

func newPoint(x int64) []interface{} {
	return []interface{}{nil, json.Number(strconv.FormatInt(x, 10)), nil}
}

func newSpecificPoint(x int64, y int64) []interface{} {
	return []interface{}{
		json.Number(strconv.FormatInt(y, 10)),
		json.Number(strconv.FormatInt(x, 10)),
		nil}
}

func newPoint1(x int64) []interface{} {
	return []interface{}{json.Number(strconv.FormatInt(x, 10)), nil}
}

func newPoint2(x int64, val float64) []interface{} {
	return []interface{}{json.Number(strconv.FormatInt(x, 10)), json.Number(strconv.FormatFloat(val, 'g', -1, 64))}
}

func TestMergeMessages(t *testing.T) {
	Convey("Merging no responses yields zero reponse", t, func() {
		resp, err := responses.Merge()
		So(*resp, ShouldBeZeroValue)
		So(err, ShouldBeNil)
	})

	Convey("Given responses with different messages", t, func() {
		message1 := &client.Message{Level: "1", Text: "one"}
		message2 := &client.Message{Level: "2", Text: "two"}
		message3 := &client.Message{Level: "3", Text: "three"}
		message4 := &client.Message{Level: "4", Text: "four"}
		message5 := &client.Message{Level: "5", Text: "five"}

		message11 := &client.Message{Level: "11", Text: "eleven"}
		message12 := &client.Message{Level: "12", Text: "twelve"}
		message13 := &client.Message{Level: "13", Text: "thirteen"}

		response1 := &client.Response{
			Results: []client.Result{
				{
					Messages: []*client.Message{
						message1,
						message2,
					},
				},
				{
					Messages: []*client.Message{
						message11,
						message12,
					},
				},
			},
		}
		response2 := &client.Response{
			Results: []client.Result{
				{
					Messages: []*client.Message{
						message3,
						message4,
						message5,
					},
				},
				{
					Messages: []*client.Message{
						message13,
					},
				},
			},
		}
		mergedResponses, err := responses.Merge(
			response1, response2)
		So(err, ShouldBeNil)
		Convey("Messages should be merged", func() {
			expected := &client.Response{
				Results: []client.Result{
					{
						Messages: []*client.Message{
							message1,
							message2,
							message3,
							message4,
							message5,
						},
					},
					{
						Messages: []*client.Message{
							message11,
							message12,
							message13,
						},
					},
				},
			}
			So(mergedResponses, ShouldResemble, expected)
		})
	})
}

func TestFromTaggedTimeSeriesSets(t *testing.T) {
	Convey("Given two ParsedQueries", t, func() {
		// We only fill in the needed fields
		pq := tsdbjson.ParsedQuery{
			Aggregator: tsdbjson.AggregatorSpec{
				DownSample: &tsdbjson.DownSampleSpec{
					DurationInSeconds: 500.0,
				},
			},
			Start: 2499.0,
			End:   5000.0,
		}
		pq2 := tsdbjson.ParsedQuery{
			Aggregator: tsdbjson.AggregatorSpec{
				DownSample: &tsdbjson.DownSampleSpec{
					DurationInSeconds: 300.0,
				},
			},
			Start: 1800.0,
			End:   2101.0,
		}
		epochConverter := func(ts int64) int64 { return ts / 2 }
		Convey("Given two simple series set", func() {
			taggedTimeSeriesSet := &tsdb.TaggedTimeSeriesSet{
				MetricName: "/my/metric",
				Data: []tsdb.TaggedTimeSeries{
					{
						Values: tsdb.TimeSeries{
							{3000.0, 30.0},
							{3500.0, 35.0},
							{4000.0, 40.0}},
					},
				},
			}
			taggedTimeSeriesSet2 := &tsdb.TaggedTimeSeriesSet{
				MetricName: "/my/metric2",
				Data: []tsdb.TaggedTimeSeries{
					{
						Values: tsdb.TimeSeries{
							{1800.0, 18.0},
							{2100.0, 21.0}},
					},
				},
			}

			result := client.Result{
				Series: []models.Row{
					{
						Name:    "/my/metric",
						Tags:    map[string]string{},
						Columns: []string{"time", "low"},
						Values: [][]interface{}{
							{int64(1000), nil},
							{int64(1250), nil},
							{int64(1500), 30.0},
							{int64(1750), 35.0},
							{int64(2000), 40.0},
							{int64(2250), nil},
						},
					},
				},
			}
			result2 := client.Result{
				Series: []models.Row{
					{
						Name:    "/my/metric2",
						Tags:    map[string]string{},
						Columns: []string{"time2", "high"},
						Values: [][]interface{}{
							{int64(900), 18.0},
							{int64(1050), 21.0},
						},
					},
				},
			}
			expected := &client.Response{
				Results: []client.Result{result, result2},
			}

			Convey("correct response", func() {
				response := responses.FromTaggedTimeSeriesSets(
					[]*tsdb.TaggedTimeSeriesSet{
						taggedTimeSeriesSet,
						taggedTimeSeriesSet2,
					},
					[][]string{
						{"time", "low"},
						{"time2", "high"},
					},
					[]tsdbjson.ParsedQuery{pq, pq2},
					epochConverter)

				So(response, ShouldResemble, expected)
			})

			Convey("correct response with a nil", func() {
				expected3 := &client.Response{
					Results: []client.Result{
						result, client.Result{}, result2},
				}
				response := responses.FromTaggedTimeSeriesSets(
					[]*tsdb.TaggedTimeSeriesSet{
						taggedTimeSeriesSet,
						nil,
						taggedTimeSeriesSet2,
					},
					[][]string{
						{"time", "low"},
						{"foo", "bar"},
						{"time2", "high"},
					},
					[]tsdbjson.ParsedQuery{pq, pq, pq2},
					epochConverter)
				So(response, ShouldResemble, expected3)
			})
		})
		Convey("Given two group by series set", func() {
			taggedTimeSeriesSet := &tsdb.TaggedTimeSeriesSet{
				MetricName: "/my/groupMetric",
				Data: []tsdb.TaggedTimeSeries{
					{
						Tags: tsdb.TagSet{
							HostName: "host1",
							AppName:  "app1",
						},
					},
					{
						Tags: tsdb.TagSet{
							HostName: "host2",
							AppName:  "app1",
						},
					},
					{
						Tags: tsdb.TagSet{
							HostName: "host1",
							AppName:  "app2",
						},
					},
					{
						Tags: tsdb.TagSet{
							HostName: "host2",
							AppName:  "app2",
						},
					},
				},
				GroupedByHostName: true,
				GroupedByAppName:  true,
			}
			result := client.Result{
				Series: []models.Row{
					{
						Name: "/my/groupMetric",
						Tags: map[string]string{
							"appname": "app1", "host": "host1"},
						Columns: []string{"foo", "bar"},
						Values: [][]interface{}{
							{int64(1000), nil},
							{int64(1250), nil},
							{int64(1500), nil},
							{int64(1750), nil},
							{int64(2000), nil},
							{int64(2250), nil},
						},
					},
					{
						Name: "/my/groupMetric",
						Tags: map[string]string{
							"appname": "app1", "host": "host2"},
						Columns: []string{"foo", "bar"},
						Values: [][]interface{}{
							{int64(1000), nil},
							{int64(1250), nil},
							{int64(1500), nil},
							{int64(1750), nil},
							{int64(2000), nil},
							{int64(2250), nil},
						},
					},
					{
						Name: "/my/groupMetric",
						Tags: map[string]string{
							"appname": "app2", "host": "host1"},
						Columns: []string{"foo", "bar"},
						Values: [][]interface{}{
							{int64(1000), nil},
							{int64(1250), nil},
							{int64(1500), nil},
							{int64(1750), nil},
							{int64(2000), nil},
							{int64(2250), nil},
						},
					},
					{
						Name: "/my/groupMetric",
						Tags: map[string]string{
							"appname": "app2", "host": "host2"},
						Columns: []string{"foo", "bar"},
						Values: [][]interface{}{
							{int64(1000), nil},
							{int64(1250), nil},
							{int64(1500), nil},
							{int64(1750), nil},
							{int64(2000), nil},
							{int64(2250), nil},
						},
					},
				},
			}
			expected := &client.Response{
				Results: []client.Result{result},
			}
			Convey("correct response", func() {
				response := responses.FromTaggedTimeSeriesSets(
					[]*tsdb.TaggedTimeSeriesSet{
						taggedTimeSeriesSet,
					},
					[][]string{
						{"foo", "bar"},
					},
					[]tsdbjson.ParsedQuery{pq},
					epochConverter)

				So(response, ShouldResemble, expected)
			})
		})
	})
}

func TestMerge(t *testing.T) {

	Convey("Given 3 responses with 2 results each", t, func() {
		valueTimeValueColumns := []string{"value1", "time", "value2"}

		apoint10 := newPoint(1010)
		apoint20 := newPoint(1020)
		apoint30 := newPoint(1030)

		bpoint5 := newPoint(2005)
		bpoint10 := newPoint(2010)
		bpoint15 := newPoint(2015)
		bpoint20 := newPoint(2020)
		bpoint25 := newPoint(2025)
		bpoint30 := newPoint(2030)

		cpoint10 := newPoint(3010)

		dpoint5 := newPoint(4005)
		dpoint10 := newPoint(4010)
		dpoint10_2 := newSpecificPoint(4010, 2)
		dpoint15 := newPoint(4015)
		dpoint15_2 := newSpecificPoint(4015, 2)
		dpoint15_3 := newSpecificPoint(4015, 3)
		dpoint25 := newPoint(4025)
		dpoint25_2 := newSpecificPoint(4025, 2)
		dpoint47 := newPoint(4047)

		epoint10 := newPoint(5010)
		epoint20 := newPoint(5020)

		a1Values := [][]interface{}{apoint10, apoint20, apoint30}
		b1Values := [][]interface{}{bpoint10, bpoint20, bpoint30}
		c1Values := [][]interface{}{cpoint10}
		d1Values := [][]interface{}{dpoint5, dpoint15, dpoint25}
		b2Values := [][]interface{}{bpoint5, bpoint15, bpoint25}
		d2Values := [][]interface{}{dpoint10, dpoint15_2, dpoint25_2}
		e2Values := [][]interface{}{epoint10, epoint20}
		d3Values := [][]interface{}{dpoint10_2, dpoint15_3, dpoint47}
		result1_1 := client.Result{
			Series: []models.Row{
				{
					Name:    "series a",
					Columns: valueTimeValueColumns,
					Values:  a1Values,
				},
				{
					Name:    "series b",
					Columns: valueTimeValueColumns,
					Values:  b1Values,
				},
				{
					Name:    "series c",
					Columns: valueTimeValueColumns,
					Values:  c1Values,
				},
				{
					Name:    "series d",
					Columns: valueTimeValueColumns,
					Values:  d1Values,
				},
			},
		}
		result1_2 := client.Result{
			Series: []models.Row{
				{
					Name:    "series b",
					Columns: valueTimeValueColumns,
					Values:  b2Values,
				},
				{
					Name:    "series d",
					Columns: valueTimeValueColumns,
					Values:  d2Values,
				},
				{
					Name:    "series e",
					Columns: valueTimeValueColumns,
					Values:  e2Values,
				},
			},
		}
		result1_3 := client.Result{
			Series: []models.Row{
				{
					Name:    "series d",
					Columns: valueTimeValueColumns,
					Values:  d3Values,
				},
			},
		}
		result2_1 := client.Result{
			Series: []models.Row{
				{
					Name: "foxtrot",
					Tags: map[string]string{
						"Bear":   "Yogi",
						"Forest": "Yosemeti",
					},
				},
				{
					Name: "foxtrot",
					Tags: map[string]string{
						"Bear":   "Yogi",
						"Forest": "Jellystone",
					},
				},
			},
		}
		result2_2 := client.Result{
			Series: []models.Row{
				{
					Name: "bravo",
					Tags: map[string]string{
						"Bear":   "Yogi",
						"Forest": "Yosemeti",
					},
				},
				{
					Name: "bravo",
					Tags: map[string]string{
						"Bear":    "Yogi",
						"Country": "Zimbabwe",
					},
				},
			},
		}
		result2_3 := client.Result{
			Series: []models.Row{
				{
					Name: "foxtrot",
					Tags: map[string]string{
						"Bear":   "Yogi",
						"Forest": "Jellystone",
						"Go":     "go",
					},
				},
				{
					Name: "foxtrot",
					Tags: map[string]string{
						"Bear": "Yogi",
					},
				},
			},
		}
		response1 := &client.Response{Results: []client.Result{result1_1, result2_1}}
		response2 := &client.Response{Results: []client.Result{result1_2, result2_2}}
		response3 := &client.Response{Results: []client.Result{result1_3, result2_3}}

		mergedResponse, err := responses.Merge(
			response1, response2, response3)
		So(err, ShouldBeNil)

		Convey("The first result from each of the three responses containing time series: A1, B1, C1, D1; B2, D2, E2; and D3 should yield merged result with time series A1, B12, C1, D123, E2.", func() {

			b12Values := [][]interface{}{
				bpoint5, bpoint10, bpoint15, bpoint20, bpoint25, bpoint30}
			d123Values := [][]interface{}{
				dpoint5,
				dpoint10_2,
				dpoint15_3,
				dpoint25_2,
				dpoint47}

			expectedResult := client.Result{
				Series: []models.Row{
					{
						Name:    "series a",
						Columns: valueTimeValueColumns,
						Values:  a1Values,
					},
					{
						Name:    "series b",
						Columns: valueTimeValueColumns,
						Values:  b12Values,
					},
					{
						Name:    "series c",
						Columns: valueTimeValueColumns,
						Values:  c1Values,
					},
					{
						Name:    "series d",
						Columns: valueTimeValueColumns,
						Values:  d123Values,
					},
					{
						Name:    "series e",
						Columns: valueTimeValueColumns,
						Values:  e2Values,
					},
				},
			}
			So(mergedResponse.Results[0], ShouldResemble, expectedResult)
		})

		Convey("The second result from each containing time series sorted by name first then by tags", func() {

			expectedResult := client.Result{
				Series: []models.Row{
					{
						Name: "bravo",
						Tags: map[string]string{
							"Bear":    "Yogi",
							"Country": "Zimbabwe",
						},
					},
					{
						Name: "bravo",
						Tags: map[string]string{
							"Bear":   "Yogi",
							"Forest": "Yosemeti",
						},
					},
					{
						Name: "foxtrot",
						Tags: map[string]string{
							"Bear": "Yogi",
						},
					},
					{
						Name: "foxtrot",
						Tags: map[string]string{
							"Bear":   "Yogi",
							"Forest": "Jellystone",
						},
					},
					{
						Name: "foxtrot",
						Tags: map[string]string{
							"Bear":   "Yogi",
							"Forest": "Jellystone",
							"Go":     "go",
						},
					},
					{
						Name: "foxtrot",
						Tags: map[string]string{
							"Bear":   "Yogi",
							"Forest": "Yosemeti",
						},
					},
				},
			}
			So(mergedResponse.Results[1], ShouldResemble, expectedResult)
		})

		Convey("Given at least one response with an error", func() {

			// Introduce a random error
			response2.Results[1].Err = "An error"
			mergedResponse, err := responses.Merge(
				response1, response2, response3)
			So(err, ShouldBeNil)
			Convey("Merged Response will have an error", func() {
				So(mergedResponse.Error(), ShouldNotBeNil)
			})
		})

		Convey("Given at least one response has partial = true", func() {

			// Introduce a random Partial = true
			response1.Results[0].Series[2].Partial = true
			_, err := responses.Merge(response1, response2, response3)

			Convey("Merge returns error", func() {
				So(err, ShouldNotBeNil)
			})

		})

		Convey("Given that each response has different result count", func() {

			response1.Results = append(response1.Results, client.Result{})
			_, err := responses.Merge(response1, response2, response3)

			Convey("Merge returns error", func() {
				So(err, ShouldNotBeNil)
			})

		})

		Convey("Given mismatched columns for same time series", func() {

			// Change series B in first response as it gets merged
			response1.Results[0].Series[1].Columns = []string{"a", "b", "c"}
			_, err := responses.Merge(response1, response2, response3)

			Convey("Merge returns error", func() {
				So(err, ShouldNotBeNil)
			})
		})

	})

}

func TestMergePreferred(t *testing.T) {
	Convey("Merge two responses", t, func() {
		poa1 := newPoint2(1000, 10.0)
		poa2 := newPoint2(1025, 10.25)
		poa3 := newPoint2(1050, 10.5)
		poa4 := newPoint2(1075, 10.75)
		poa5 := newPoint2(1100, 11.0)
		poa6 := newPoint2(1125, 11.25)
		poa7 := newPoint2(1150, 11.5)
		poa8 := newPoint2(1175, 11.75)

		ppa1 := newPoint1(1000.0)
		ppa2 := newPoint1(1010.0)
		ppa3 := newPoint1(1025.0)
		ppa4 := newPoint1(1035.0)
		ppa5 := newPoint1(1050.0)
		ppa6 := newPoint1(1075.0)
		ppa7 := newPoint2(1100, 21.0)
		ppa8 := newPoint2(1125, 21.25)
		ppa9 := newPoint2(1150, 21.5)
		ppa10 := newPoint2(1175, 0.0)

		pob1 := newPoint2(2000, 30.0)
		pob2 := newPoint2(2025, 30.25)
		pob3 := newPoint2(2050, 30.5)
		pob4 := newPoint2(2075, 30.75)
		pob5 := newPoint2(2100, 31.0)
		pob6 := newPoint2(2125, 31.25)
		pob7 := newPoint2(2150, 31.5)
		pob8 := newPoint2(2175, 31.75)

		ppb1 := newPoint2(2000.0, 0.0)
		ppb2 := newPoint2(2010.0, 0.0)
		ppb3 := newPoint2(2025.0, 0.0)
		ppb4 := newPoint2(2035.0, 0.0)
		ppb5 := newPoint2(2050.0, 0.0)
		ppb6 := newPoint2(2075.0, 0.0)
		ppb7 := newPoint2(2100, 41.0)
		ppb8 := newPoint2(2125, 41.25)
		ppb9 := newPoint2(2150, 41.5)
		ppb10 := newPoint2(2175, 0.0)

		poc1 := newPoint2(9919, 91.0)
		poc2 := newPoint2(9929, 92.0)

		ppc1 := newPoint2(9919, 11.0)
		ppc2 := newPoint2(9929, 22.0)

		pod1 := newPoint2(5000, 50.0)
		pod2 := newPoint2(5100, 51.0)

		ppe1 := newPoint2(6000, 60.0)
		ppe2 := newPoint2(6100, 61.0)

		oaValues := [][]interface{}{
			poa3, poa6, poa1, poa4, poa7, poa2, poa5, poa8}
		paValues := [][]interface{}{
			ppa7, ppa4, ppa1, ppa8, ppa5, ppa2, ppa9, ppa6, ppa3, ppa10}
		obValues := [][]interface{}{
			pob3, pob6, pob1, pob4, pob7, pob2, pob5, pob8}
		pbValues := [][]interface{}{
			ppb7, ppb4, ppb1, ppb8, ppb5, ppb2, ppb9, ppb6, ppb3, ppb10}
		ocValues := [][]interface{}{poc2, poc1}
		pcValues := [][]interface{}{ppc1, ppc2}
		odValues := [][]interface{}{pod1, pod2}
		peValues := [][]interface{}{ppe2, ppe1}

		oaSeries := models.Row{
			Name:    "series_a",
			Columns: []string{"time", "value"},
			Values:  oaValues,
		}
		paSeries := models.Row{
			Name:    "series_a",
			Columns: []string{"time", "value"},
			Values:  paValues,
		}

		obSeries := models.Row{
			Name:    "series_b",
			Columns: []string{"time", "value"},
			Values:  obValues,
		}
		pbSeries := models.Row{
			Name:    "series_b",
			Columns: []string{"time", "value"},
			Values:  pbValues,
		}

		ocSeries := models.Row{
			Name:    "series_c",
			Columns: []string{"nottime", "value"},
			Values:  ocValues,
		}
		pcSeries := models.Row{
			Name:    "series_c",
			Columns: []string{"nottime", "value"},
			Values:  pcValues,
		}

		odSeries := models.Row{
			Name:    "series_d",
			Columns: []string{"time", "value"},
			Values:  odValues,
		}
		peSeries := models.Row{
			Name:    "series_e",
			Columns: []string{"nottime", "value"},
			Values:  peValues,
		}

		oResult := client.Result{
			Series: []models.Row{
				oaSeries,
				obSeries,
				ocSeries,
				odSeries,
			},
		}
		pResult := client.Result{
			Series: []models.Row{
				paSeries,
				pbSeries,
				pcSeries,
				peSeries,
			},
		}

		oResponse := &client.Response{Results: []client.Result{oResult}}
		pResponse := &client.Response{Results: []client.Result{pResult}}
		Convey("Result should be correct", func() {
			expectedResult := client.Result{
				Series: []models.Row{
					{
						Name:    "series_a",
						Columns: []string{"time", "value"},
						Values: [][]interface{}{
							poa1,
							poa2,
							poa3,
							poa4,
							ppa7,
							ppa8,
							ppa9,
							ppa10,
						},
					},
					{
						Name:    "series_b",
						Columns: []string{"time", "value"},
						Values: [][]interface{}{
							pob1,
							pob2,
							pob3,
							pob4,
							ppb7,
							ppb8,
							ppb9,
							ppb10,
						},
					},
					{
						Name:    "series_c",
						Columns: []string{"nottime", "value"},
						Values: [][]interface{}{
							poc2,
							poc1,
						},
					},
					{
						Name:    "series_d",
						Columns: []string{"time", "value"},
						Values: [][]interface{}{
							pod1,
							pod2,
						},
					},
					{
						Name:    "series_e",
						Columns: []string{"nottime", "value"},
						Values: [][]interface{}{
							ppe2,
							ppe1,
						},
					},
				},
			}
			actual, err := responses.MergePreferred(oResponse, pResponse)
			So(err, ShouldBeNil)
			So(actual.Results[0], ShouldResemble, expectedResult)

		})

	})

}
