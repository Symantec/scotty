package preference_test

import (
	"github.com/Symantec/scotty/lib/preference"
	"reflect"
	"testing"
)

func TestNoReset(t *testing.T) {
	p := preference.New(6, 0)
	p.SetFirstIndex(4)
	assertValueEquals(t, 4, p.FirstIndex())
	p.SetFirstIndex(4)
	assertValueEquals(t, 4, p.FirstIndex())
	p.SetFirstIndex(4)
	assertValueEquals(t, 4, p.FirstIndex())
	p.SetFirstIndex(4)
	assertValueEquals(t, 4, p.FirstIndex())
	p.SetFirstIndex(1)
	assertValueEquals(t, 1, p.FirstIndex())
	p.SetFirstIndex(0)
	assertValueEquals(t, 0, p.FirstIndex())

	p = preference.New(3, -1)
	p.SetFirstIndex(2)
	assertValueEquals(t, 2, p.FirstIndex())
	p.SetFirstIndex(2)
	assertValueEquals(t, 2, p.FirstIndex())
	p.SetFirstIndex(2)
	assertValueEquals(t, 2, p.FirstIndex())
}

func TestAPI(t *testing.T) {
	p := preference.New(5, 3)
	assertValueEquals(t, 0, p.FirstIndex())
	assertValueDeepEquals(
		t, []int{0, 1, 2, 3, 4}, p.Indexes())
	// 1st consecutive call for 4
	p.SetFirstIndex(4)
	// 2nd consecutive call for 4
	p.SetFirstIndex(4)
	assertValueEquals(t, 4, p.FirstIndex())
	assertValueEquals(t, 4, p.FirstIndex())
	assertValueEquals(t, 4, p.FirstIndex())
	assertValueEquals(t, 4, p.FirstIndex())
	assertValueDeepEquals(
		t, []int{4, 0, 1, 2, 3}, p.Indexes())
	assertValueDeepEquals(
		t, []int{4, 0, 1, 2, 3}, p.Indexes())
	assertValueDeepEquals(
		t, []int{4, 0, 1, 2, 3}, p.Indexes())
	assertValueDeepEquals(
		t, []int{4, 0, 1, 2, 3}, p.Indexes())
	assertValueEquals(t, 4, p.FirstIndex())
	// 1st consecutive call for 1
	p.SetFirstIndex(1)
	assertValueEquals(t, 1, p.FirstIndex())
	assertValueDeepEquals(
		t, []int{1, 0, 2, 3, 4}, p.Indexes())
	// 2st consecutive call for 1
	p.SetFirstIndex(1)
	// 3rd consecutive call for 1 resets FirstIndex to 0
	p.SetFirstIndex(1)
	assertValueEquals(t, 0, p.FirstIndex())
	assertValueDeepEquals(
		t, []int{0, 1, 2, 3, 4}, p.Indexes())
	// 4th consecutive call for 1 becomes like the 1st consecutive call
	p.SetFirstIndex(1)
	assertValueEquals(t, 1, p.FirstIndex())
	assertValueDeepEquals(
		t, []int{1, 0, 2, 3, 4}, p.Indexes())
	// 2 consecutive calls for 3
	p.SetFirstIndex(3)
	p.SetFirstIndex(3)
	assertValueEquals(t, 3, p.FirstIndex())
	assertValueDeepEquals(
		t, []int{3, 0, 1, 2, 4}, p.Indexes())
	// 2 consecutive calls for 2
	p.SetFirstIndex(2)
	p.SetFirstIndex(2)
	assertValueEquals(t, 2, p.FirstIndex())
	assertValueDeepEquals(
		t, []int{2, 0, 1, 3, 4}, p.Indexes())
	// 3 consecutive call for 2
	p.SetFirstIndex(2)
	assertValueEquals(t, 0, p.FirstIndex())
	assertValueDeepEquals(
		t, []int{0, 1, 2, 3, 4}, p.Indexes())
	// 3 consecutive call for 0 still 0
	p.SetFirstIndex(0)
	p.SetFirstIndex(0)
	p.SetFirstIndex(0)
	assertValueEquals(t, 0, p.FirstIndex())
	assertValueDeepEquals(
		t, []int{0, 1, 2, 3, 4}, p.Indexes())
}

func assertValueEquals(t *testing.T, expected, actual interface{}) bool {
	if expected != actual {
		t.Errorf("Expected %v, got %v", expected, actual)
		return false
	}
	return true
}

func assertValueDeepEquals(
	t *testing.T, expected, actual interface{}) bool {
	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("Expected %v, got %v", expected, actual)
		return false
	}
	return true
}
