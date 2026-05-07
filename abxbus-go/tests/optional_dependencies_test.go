package abxbus_test

import (
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"

	abxbus "github.com/ArchiveBox/abxbus/abxbus-go"
)

func TestGoIntegrationSurfaceOnlyIncludesSupportedBridgeAndMiddleware(t *testing.T) {
	var _ = abxbus.NewJSONLEventBridge
	var _ abxbus.EventBusMiddleware = abxbus.NewOtelTracingMiddleware(nil)

	goRoot := goPackageRoot(t)
	entries, err := os.ReadDir(goRoot)
	if err != nil {
		t.Fatal(err)
	}

	bridgeFiles := map[string]bool{}
	middlewareFiles := map[string]bool{}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") {
			continue
		}
		switch {
		case strings.HasSuffix(entry.Name(), "_bridge.go"):
			bridgeFiles[entry.Name()] = true
		case strings.HasSuffix(entry.Name(), "_middleware.go") || entry.Name() == "middleware.go":
			middlewareFiles[entry.Name()] = true
		}
	}

	if len(bridgeFiles) != 1 || !bridgeFiles["jsonl_bridge.go"] {
		t.Fatalf("Go should only implement JSONL bridge for now, got %v", bridgeFiles)
	}
	expectedMiddlewareFiles := map[string]bool{"middleware.go": true, "otel_middleware.go": true}
	if len(middlewareFiles) != len(expectedMiddlewareFiles) {
		t.Fatalf("unexpected Go middleware files: %v", middlewareFiles)
	}
	for expected := range expectedMiddlewareFiles {
		if !middlewareFiles[expected] {
			t.Fatalf("missing expected Go middleware file %s, got %v", expected, middlewareFiles)
		}
	}
}

func TestGoUnsupportedBridgeAPIsAndDependenciesAreAbsent(t *testing.T) {
	goRoot := goPackageRoot(t)
	sourceFiles, err := filepath.Glob(filepath.Join(goRoot, "*.go"))
	if err != nil {
		t.Fatal(err)
	}
	unsupportedAPI := regexp.MustCompile(`\b(?:New)?(?:HTTP|Socket|SQLite|Redis|NATS|Postgres|Tachyon)EventBridge\b`)
	for _, path := range sourceFiles {
		source, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if unsupportedAPI.Match(source) {
			t.Fatalf("unsupported Go bridge API leaked into %s", filepath.Base(path))
		}
	}

	for _, filename := range []string{"go.mod", "go.sum"} {
		data, err := os.ReadFile(filepath.Join(goRoot, filename))
		if err != nil {
			t.Fatal(err)
		}
		text := string(data)
		for _, forbidden := range []string{
			"github.com/jackc/pgx",
			"github.com/lib/pq",
			"github.com/redis",
			"github.com/nats-io",
			"modernc.org/sqlite",
			"github.com/mattn/go-sqlite3",
			"tachyon",
		} {
			if strings.Contains(text, forbidden) {
				t.Fatalf("%s unexpectedly references unsupported optional integration dependency %q", filename, forbidden)
			}
		}
	}
}

func goPackageRoot(t *testing.T) string {
	t.Helper()
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Dir(filepath.Dir(filename))
}
