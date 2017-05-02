package cloudhealth

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Symantec/scotty/lib/httputil"
	"net/http"
	"time"
)

var (
	kErrTooManyPoints = errors.New("Too many data points")
)

type metaDataType struct {
	AssetType   string   `json:"assetType"`
	Granularity string   `json:"granularity"`
	Keys        []string `json:"keys"`
}

type datasetType struct {
	Metadata *metaDataType   `json:"metadata"`
	Values   [][]interface{} `json:"values"`
}

type metricType struct {
	Datasets []datasetType `json:"datasets"`
}

type requestType struct {
	Metrics metricType `json:"metrics"`
}

var kInstance = &metaDataType{
	AssetType:   "aws:ec2:instance",
	Granularity: "hour",
	Keys: []string{
		"assetId",
		"timestamp",
		"cpu:used:percent.avg",
		"cpu:used:percent.max",
		"cpu:used:percent.min",
		"memory:free:bytes.avg",
		"memory:free:bytes.max",
		"memory:free:bytes.min",
		"memory:size:bytes.avg",
		"memory:size:bytes.max",
		"memory:size:bytes.min",
		"memory:used:percent.avg",
		"memory:used:percent.max",
		"memory:used:percent.min",
	},
}

var kFs = &metaDataType{
	AssetType:   "aws:ec2:instance:fs",
	Granularity: "hour",
	Keys: []string{
		"assetId",
		"timestamp",
		"fs:size:bytes.avg",
		"fs:size:bytes.max",
		"fs:size:bytes.min",
		"fs:used:bytes.avg",
		"fs:used:bytes.max",
		"fs:used:bytes.min",
		"fs:used:percent.avg",
		"fs:used:percent.max",
		"fs:used:percent.min",
	},
}

type chResponseType struct {
	Succeeded int `json:"succeeded"`
	Failed    int `json:"failed"`
	Errors    int `json:"errors"`
}

func (v FVariable) toValues() (avg, max, min interface{}) {
	if v.Count == 0 {
		return
	}
	return v.Avg(), v.Max, v.Min
}

func (v IVariable) toValues() (avg, max, min interface{}) {
	if v.Count == 0 {
		return
	}
	return v.Avg(), v.Max, v.Min
}

func toTsValue(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05-07:00")
}

func (d *InstanceData) toValues(
	dataCenter, accountNumber string) []interface{} {
	accountNumberToUse := accountNumber
	if d.AccountNumber != "" {
		accountNumberToUse = d.AccountNumber
	}
	assetId := fmt.Sprintf(
		"%s:%s:%s", dataCenter, accountNumberToUse, d.InstanceId)
	cpuUsedPercentAvg, cpuUsedPercentMax, cpuUsedPercentMin :=
		d.CpuUsedPercent.toValues()
	memoryFreeBytesAvg, memoryFreeBytesMax, memoryFreeBytesMin :=
		d.MemoryFreeBytes.toValues()
	memorySizeBytesAvg, memorySizeBytesMax, memorySizeBytesMin :=
		d.MemorySizeBytes.toValues()
	memoryUsedPercentAvg, memoryUsedPercentMax, memoryUsedPercentMin :=
		d.MemoryUsedPercent.toValues()
	return []interface{}{
		assetId,
		toTsValue(d.Ts),
		cpuUsedPercentAvg,
		cpuUsedPercentMax,
		cpuUsedPercentMin,
		memoryFreeBytesAvg,
		memoryFreeBytesMax,
		memoryFreeBytesMin,
		memorySizeBytesAvg,
		memorySizeBytesMax,
		memorySizeBytesMin,
		memoryUsedPercentAvg,
		memoryUsedPercentMax,
		memoryUsedPercentMin,
	}
}

func (d *FsData) toValues(
	dataCenter, accountNumber string) []interface{} {
	accountNumberToUse := accountNumber
	if d.AccountNumber != "" {
		accountNumberToUse = d.AccountNumber
	}
	assetId := fmt.Sprintf(
		"%s:%s:%s:%s",
		dataCenter,
		accountNumberToUse,
		d.InstanceId,
		d.MountPoint)
	fsSizeBytesAvg, fsSizeBytesMax, fsSizeBytesMin :=
		d.FsSizeBytes.toValues()
	fsUsedBytesAvg, fsUsedBytesMax, fsUsedBytesMin :=
		d.FsUsedBytes.toValues()
	fsUsedPercentAvg, fsUsedPercentMax, fsUsedPercentMin :=
		d.FsUsedPercent.toValues()
	return []interface{}{
		assetId,
		toTsValue(d.Ts),
		fsSizeBytesAvg,
		fsSizeBytesMax,
		fsSizeBytesMin,
		fsUsedBytesAvg,
		fsUsedBytesMax,
		fsUsedBytesMin,
		fsUsedPercentAvg,
		fsUsedPercentMax,
		fsUsedPercentMin,
	}
}

func (w *Writer) buildRequest(
	instances []InstanceData, fss []FsData) ([]byte, error) {
	var metrics []datasetType
	if len(instances) > 0 {
		instanceValues := make([][]interface{}, len(instances))
		for i := range instances {
			instanceValues[i] = instances[i].toValues(
				w.config.DataCenter, w.config.AccountNumber)
		}
		metrics = append(
			metrics,
			datasetType{
				Metadata: kInstance,
				Values:   instanceValues,
			})
	}
	if len(fss) > 0 {
		fsValues := make([][]interface{}, len(fss))
		for i := range fss {
			fsValues[i] = fss[i].toValues(
				w.config.DataCenter, w.config.AccountNumber)
		}
		metrics = append(
			metrics,
			datasetType{
				Metadata: kFs,
				Values:   fsValues,
			})
	}
	return json.MarshalIndent(
		&requestType{
			Metrics: metricType{
				Datasets: metrics,
			},
		}, "", "\t")
}

func (w *Writer) write(
	instances []InstanceData, fss []FsData) (int, error) {
	if len(instances)*InstanceDataPointCount+len(fss)*FsDataPointCount > MaxDataPoints {
		return 0, kErrTooManyPoints
	}
	body, err := w.buildRequest(instances, fss)
	if err != nil {
		return 0, err
	}
	url := httputil.NewUrl(
		"https://chapi.cloudhealthtech.com/metrics/v1",
		"api_key",
		w.config.ApiKey)
	if w.config.DryRun {
		url = httputil.AppendParams(
			url,
			"dryrun",
			"true")
	}
	urlStr := url.String()
	var client http.Client
	resp, err := client.Post(
		urlStr, "application/json", bytes.NewBuffer(body))
	if err != nil {
		return 0, err
	}
	respCode, err := parseResponse(resp)
	delay := 20 * time.Millisecond
	for respCode == 429 {
		time.Sleep(delay)
		delay *= 2
		resp, err = client.Post(
			urlStr, "application/json", bytes.NewBuffer(body))
		if err != nil {
			return 0, err
		}
		respCode, err = parseResponse(resp)
	}
	return respCode, err
}

func parseResponse(resp *http.Response) (int, error) {
	defer resp.Body.Close()
	var buffer bytes.Buffer
	buffer.ReadFrom(resp.Body)
	if resp.StatusCode/100 != 2 {
		return resp.StatusCode, errors.New(buffer.String())
	}
	var chResponse chResponseType
	json.Unmarshal(buffer.Bytes(), &chResponse)
	if chResponse.Failed > 0 || chResponse.Errors > 0 {
		return resp.StatusCode, errors.New(buffer.String())
	}
	return resp.StatusCode, nil
}
