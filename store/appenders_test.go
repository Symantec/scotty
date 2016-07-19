package store

import (
	"reflect"
	"testing"
)

func TestMergeNewestFirst(t *testing.T) {
	series := [][]Record{
		{
			{TimeStamp: 999.0},
			{TimeStamp: 666.0},
			{TimeStamp: 333.0},
			{Value: 3, TimeStamp: 222.0},
		},
		{},
		{
			{TimeStamp: 777.0},
		},
		{
			{TimeStamp: 985.0},
			{TimeStamp: 923.0},
			{TimeStamp: 222.0, Active: true},
		},
	}
	var result []Record
	mergeNewestFirst(series, AppendTo(&result))
	expected := []Record{
		{TimeStamp: 999.0},
		{TimeStamp: 985.0},
		{TimeStamp: 923.0},
		{TimeStamp: 777.0},
		{TimeStamp: 666.0},
		{TimeStamp: 333.0},
		{TimeStamp: 222.0, Active: true},
	}
	if !reflect.DeepEqual(expected, result) {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

func TestMergeOldestFirst(t *testing.T) {
	series := [][]Record{
		{
			{TimeStamp: 222.0, Active: true},
			{TimeStamp: 333.0},
			{TimeStamp: 666.0},
			{TimeStamp: 999.0},
		},
		{},
		{
			{TimeStamp: 777.0},
		},
		{
			{Value: 3, TimeStamp: 222.0},
			{TimeStamp: 923.0},
			{TimeStamp: 985.0},
		},
	}
	var result []Record
	mergeOldestFirst(series, AppendTo(&result))
	expected := []Record{
		{TimeStamp: 222.0, Active: true},
		{TimeStamp: 333.0},
		{TimeStamp: 666.0},
		{TimeStamp: 777.0},
		{TimeStamp: 923.0},
		{TimeStamp: 985.0},
		{TimeStamp: 999.0},
	}
	if !reflect.DeepEqual(expected, result) {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}
