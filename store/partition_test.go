package store

import (
	"reflect"
	"testing"
)

type intPartitionForTestingType []int

func (p intPartitionForTestingType) Swap(i, j int) {
	p[j], p[i] = p[i], p[j]
}

func (p intPartitionForTestingType) SubsetId(index int) interface{} {
	return p[index] / 10
}

func (p intPartitionForTestingType) Len() int {
	return len(p)
}

func TestPartitionScrambled(t *testing.T) {
	scrambled := intPartitionForTestingType{
		44, 32, 43, 42, 41, 54, 21, 3, 53, 2, 31, 1, 52, 51}
	formPartition(scrambled)
	verifyPartition(t, scrambled)
}

func TestPartitionPreserveOrderWhenPossible(t *testing.T) {
	ordered := intPartitionForTestingType{
		44, 32, 31, 7, 6, 5, 19, 17, 23}
	expected := make(intPartitionForTestingType, len(ordered))
	copy(expected, ordered)
	formPartition(ordered)
	if !reflect.DeepEqual(expected, ordered) {
		t.Error("Expect formPartition to preserve order when possible")
	}
	verifyPartition(t, ordered)
}

func TestPartitionPreserveOrderWhenPossibleOnePartition(t *testing.T) {
	ordered := intPartitionForTestingType{
		109, 101, 108, 102, 107, 103, 106}
	expected := make(intPartitionForTestingType, len(ordered))
	copy(expected, ordered)
	formPartition(ordered)
	if !reflect.DeepEqual(expected, ordered) {
		t.Error("Expect formPartition to preserve order when possible")
	}
	verifyPartition(t, ordered)
}

func verifyPartition(t *testing.T, partition intPartitionForTestingType) {
	length := len(partition)
	groupsvisited := make(map[int]bool)
	needToVisit := make(map[int]bool, length)
	for i := range partition {
		needToVisit[partition[i]] = true
	}
	for startIdx, endIdx := 0, 0; startIdx < length; startIdx = endIdx {
		endIdx = nextSubset(partition, startIdx)
		group := partition[startIdx:endIdx]
		if groupsvisited[group[0]/10] {
			t.Errorf("Group visited: %v", group)
		}
		groupsvisited[group[0]/10] = true
		notAGroup := false
		for i := range group {
			if !needToVisit[group[i]] {
				t.Errorf("Unexpected group memeber: %d", group[i])
			}
			delete(needToVisit, group[i])
			if group[i]/10 != group[0]/10 {
				notAGroup = true
			}
		}
		if notAGroup {
			t.Errorf("Not a group: %v", group)
		}
	}
	assertValueEquals(t, 0, len(needToVisit))
}

func assertValueEquals(t *testing.T, expected, actual interface{}) bool {
	if expected != actual {
		t.Errorf("Expected %v, got %v", expected, actual)
		return false
	}
	return true
}
