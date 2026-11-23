package abxbus

import (
	"os"
	"path/filepath"
	"testing"
)

func TestJSONLReadWindowDoesNotReplayConcurrentAppend(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	first, second := "{\"label\":\"first\"}\n", "{\"label\":\"second\"}\n"
	if err := os.WriteFile(path, []byte(first), 0o600); err != nil {
		t.Fatal(err)
	}
	reader, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	observed, err := reader.Stat()
	if err != nil {
		t.Fatal(err)
	}
	writer, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, writeErr := writer.WriteString(second)
	closeErr := writer.Close()
	if writeErr != nil || closeErr != nil {
		t.Fatalf("append: %v; close: %v", writeErr, closeErr)
	}
	got, next, err := readJSONLWindow(reader, 0, observed.Size())
	if err != nil || got != first || next != int64(len(first)) {
		t.Fatalf("first read got %q, offset %d, err %v; want only first record", got, next, err)
	}
	got, next, err = readJSONLWindow(reader, next, int64(len(first)+len(second)))
	if err != nil || got != second || next != int64(len(first)+len(second)) {
		t.Fatalf("second read got %q, offset %d, err %v; want only appended record", got, next, err)
	}
}
