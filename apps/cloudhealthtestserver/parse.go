package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Symantec/scotty/cloudhealthlmm"
	"io"
	"strings"
	"time"
)

const (
	kFsType       = "aws:ec2:instance:fs"
	kInstanceType = "aws:ec2:instance"
)

var (
	kInstanceNamesToLmm = map[string]string{
		"cpu:used:percent.avg":    cloudhealthlmm.CpuUsedPercentAvg,
		"cpu:used:percent.max":    cloudhealthlmm.CpuUsedPercentMax,
		"cpu:used:percent.min":    cloudhealthlmm.CpuUsedPercentMin,
		"memory:free:bytes.avg":   cloudhealthlmm.MemoryFreeBytesAvg,
		"memory:free:bytes.max":   cloudhealthlmm.MemoryFreeBytesMax,
		"memory:free:bytes.min":   cloudhealthlmm.MemoryFreeBytesMin,
		"memory:size:bytes.avg":   cloudhealthlmm.MemorySizeBytesAvg,
		"memory:size:bytes.max":   cloudhealthlmm.MemorySizeBytesMax,
		"memory:size:bytes.min":   cloudhealthlmm.MemorySizeBytesMin,
		"memory:used:percent.avg": cloudhealthlmm.MemoryUsedPercentAvg,
		"memory:used:percent.max": cloudhealthlmm.MemoryUsedPercentMax,
		"memory:used:percent.min": cloudhealthlmm.MemoryUsedPercentMin,
	}

	kFssNamesToLmm = map[string]string{
		"fs:size:bytes.avg":   cloudhealthlmm.FsSizeBytesAvg,
		"fs:size:bytes.max":   cloudhealthlmm.FsSizeBytesMax,
		"fs:size:bytes.min":   cloudhealthlmm.FsSizeBytesMin,
		"fs:used:bytes.avg":   cloudhealthlmm.FsUsedBytesAvg,
		"fs:used:bytes.max":   cloudhealthlmm.FsUsedBytesMax,
		"fs:used:bytes.min":   cloudhealthlmm.FsUsedBytesMin,
		"fs:used:percent.avg": cloudhealthlmm.FsUsedPercentAvg,
		"fs:used:percent.max": cloudhealthlmm.FsUsedPercentMax,
		"fs:used:percent.min": cloudhealthlmm.FsUsedPercentMin,
	}
)

var (
	kErrMissingMetaData  = errors.New("Metadata chunk missing")
	kErrMissingAssetId   = errors.New("Missing Asset Id")
	kErrMissingTimestamp = errors.New("Missing timestamp")
	kErrTooFewValues     = errors.New("Too few values")
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

func extractRequest(reader io.Reader) (*requestType, error) {
	decoder := json.NewDecoder(reader)
	var result *requestType
	if err := decoder.Decode(&result); err != nil {
		return nil, err
	}
	return result, nil
}

type lmmMetricType struct {
	Region        string
	AccountNumber string
	InstanceId    string
	Date          time.Time
	MetricName    string
	Value         float64
}

func extractMetricsFromBody(body io.Reader) ([]lmmMetricType, error) {
	req, err := extractRequest(body)
	if err != nil {
		return nil, err
	}
	result, err := extractMetrics(req)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func extractMetrics(req *requestType) ([]lmmMetricType, error) {
	var result []lmmMetricType
	for _, dataset := range req.Metrics.Datasets {
		if dataset.Metadata == nil {
			return nil, kErrMissingMetaData
		}
		if dataset.Metadata.AssetType == kInstanceType {
			if err := extractInstanceMetrics(
				dataset.Metadata.Keys, dataset.Values, &result); err != nil {
				return nil, err
			}
		} else if dataset.Metadata.AssetType == kFsType {
			if err := extractFsMetrics(
				dataset.Metadata.Keys, dataset.Values, &result); err != nil {
				return nil, err
			}
		}
	}
	return result, nil
}

func maybeExtractSingleMetric(
	assetId string,
	timestamp time.Time,
	group []interface{},
	idx int,
	metricName string,
	sink *[]lmmMetricType) error {
	if idx < 0 {
		return nil
	}
	if group[idx] == nil {
		return nil
	}
	val, ok := group[idx].(float64)
	if !ok {
		return fmt.Errorf("%v should be a float.", group[idx])
	}
	region, accountNumber, instanceId, err := splitUpId(assetId)
	if err != nil {
		return err
	}
	*sink = append(
		*sink,
		lmmMetricType{
			Region:        region,
			AccountNumber: accountNumber,
			InstanceId:    instanceId,
			Date:          timestamp,
			MetricName:    metricName,
			Value:         val,
		})
	return nil
}

func extractAssetIdAndTimestamp(
	assetIdIdx, timestampIdx int, group []interface{}) (
	assetId string, timestamp time.Time, err error) {
	if assetIdIdx == -1 {
		err = kErrMissingAssetId
		return
	}
	if timestampIdx == -1 {
		err = kErrMissingTimestamp
		return
	}
	var ok bool
	assetId, ok = group[assetIdIdx].(string)
	if !ok {
		err = fmt.Errorf("assetId: %v should be a string", group[assetIdIdx])
		return
	}
	timestampStr, ok := group[timestampIdx].(string)
	if !ok {
		err = fmt.Errorf("timestamp: %v should be a string", group[timestampIdx])
		return
	}
	timestamp, err = time.Parse("2006-01-02T15:04:05-07:00", timestampStr)
	return
}

type namedIndexType struct {
	Name  string
	Index int
}

func extractInstanceMetrics(
	keys []string, values [][]interface{}, sink *[]lmmMetricType) error {
	assetIdIdx := -1
	timestampIdx := -1
	var namedIndexes []namedIndexType
	for idx, key := range keys {
		switch key {
		case "assetId":
			assetIdIdx = idx
		case "timestamp":
			timestampIdx = idx
		default:
			lmmName, ok := kInstanceNamesToLmm[key]
			if ok {
				namedIndexes = append(
					namedIndexes,
					namedIndexType{Name: lmmName, Index: idx})
			}
		}
	}
	for _, group := range values {
		if len(group) < len(keys) {
			return kErrTooFewValues
		}
		assetId, timestamp, err := extractAssetIdAndTimestamp(
			assetIdIdx, timestampIdx, group)
		if err != nil {
			return err
		}
		for _, namedIndex := range namedIndexes {
			if err := maybeExtractSingleMetric(
				assetId,
				timestamp,
				group,
				namedIndex.Index,
				namedIndex.Name,
				sink); err != nil {
				return err
			}
		}
	}
	return nil
}

func extractMountPoint(assetId string) (instanceId, mountPoint string) {
	idx := strings.LastIndex(assetId, ":")
	if idx == -1 {
		return assetId, ""
	}
	return assetId[:idx], assetId[(idx + 1):]
}

func splitUpId(id string) (region, accountNumber, instanceId string, err error) {
	parts := strings.Split(id, ":")
	if len(parts) < 3 {
		err = fmt.Errorf("Malformed ID: %s", id)
		return
	}
	region = parts[0]
	accountNumber = parts[1]
	instanceId = parts[2]
	return
}

func extractFsMetrics(
	keys []string, values [][]interface{}, sink *[]lmmMetricType) error {
	assetIdIdx := -1
	timestampIdx := -1
	var namedIndexes []namedIndexType
	for idx, key := range keys {
		switch key {
		case "assetId":
			assetIdIdx = idx
		case "timestamp":
			timestampIdx = idx
		default:
			lmmName, ok := kFssNamesToLmm[key]
			if ok {
				namedIndexes = append(
					namedIndexes,
					namedIndexType{Name: lmmName, Index: idx})
			}
		}
	}
	for _, group := range values {
		if len(group) < len(keys) {
			return kErrTooFewValues
		}
		assetId, timestamp, err := extractAssetIdAndTimestamp(
			assetIdIdx, timestampIdx, group)
		if err != nil {
			return err
		}
		assetId, mountPoint := extractMountPoint(assetId)
		if mountPoint != "/" {
			continue
		}
		for _, namedIndex := range namedIndexes {
			if err := maybeExtractSingleMetric(
				assetId,
				timestamp,
				group,
				namedIndex.Index,
				namedIndex.Name,
				sink); err != nil {
				return err
			}
		}
		break
	}
	return nil
}
