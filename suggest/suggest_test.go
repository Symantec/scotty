package suggest_test

import (
	"github.com/Symantec/scotty/suggest"
	"reflect"
	"testing"
)

func TestEngine(t *testing.T) {
	engine := suggest.NewEngine()
	engine.Add("A")
	engine.Add("Hi")
	engine.Add("Hello")
	engine.Add("Suggest")
	engine.Add("A")
	engine.Add("Hi")
	engine.Add("Hello")
	engine.Add("Suggest")
	engine.Await()
	assertValueDeepEquals(
		t, []string{"Hello", "Hi"}, engine.Suggest(3, "H"))
	assertValueDeepEquals(
		t, []string{"Hello", "Hi"}, engine.Suggest(2, "H"))
	assertValueDeepEquals(
		t, []string{"Hello"}, engine.Suggest(1, "H"))
	assertValueDeepEquals(
		t, []string{"Hello", "Hi"}, engine.Suggest(0, "H"))
	assertValueDeepEquals(
		t, []string{"A", "Hello", "Hi", "Suggest"}, engine.Suggest(0, ""))
	assertValueEquals(t, 0, len(engine.Suggest(0, "J")))
	assertValueEquals(t, 0, len(engine.Suggest(5, "J")))

}

func TestConstEngine(t *testing.T) {
	engine := suggest.NewSuggester(
		"log", "logger", "loggest", "a", "an", "and", "aback")
	assertValueDeepEquals(
		t, []string{"log", "logger", "loggest"}, engine.Suggest(0, "log"))
	assertValueDeepEquals(
		t, []string{"log", "logger", "loggest"}, engine.Suggest(3, "log"))
	assertValueDeepEquals(
		t, []string{"log", "logger"}, engine.Suggest(2, "log"))
	assertValueDeepEquals(
		t, []string{"log"}, engine.Suggest(1, "log"))
	assertValueDeepEquals(
		t, []string{"logger", "loggest"}, engine.Suggest(2, "logg"))
	assertValueDeepEquals(
		t, []string{"a", "an", "and", "aback"}, engine.Suggest(0, "a"))
	assertValueDeepEquals(
		t, []string{"a", "an", "and", "aback"}, engine.Suggest(0, "a"))
	assertValueDeepEquals(
		t, []string{"aback"}, engine.Suggest(0, "ab"))
	assertValueEquals(t, 0, len(engine.Suggest(0, "abc")))
	assertValueEquals(t, 0, len(engine.Suggest(0, "m")))
	assertValueEquals(t, 0, len(engine.Suggest(5, "m")))

}
func assertValueEquals(
	t *testing.T, expected, actual interface{}) bool {
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
