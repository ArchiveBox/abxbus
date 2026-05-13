package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	abxbus "github.com/ArchiveBox/abxbus/abxbus-go"
)

func usage() {
	fmt.Fprintln(os.Stderr, "usage: abxbus-go-roundtrip <events|bus> <input.json> <output.json>")
	fmt.Fprintln(os.Stderr, "       abxbus-go-roundtrip jsonl-listener <config.json>")
	os.Exit(2)
}

type jsonlListenerConfig struct {
	Path       string `json:"path"`
	ReadyPath  string `json:"ready_path"`
	OutputPath string `json:"output_path"`
}

func main() {
	if len(os.Args) < 2 {
		usage()
	}

	mode := os.Args[1]
	if mode == "jsonl-listener" {
		if len(os.Args) != 3 {
			usage()
		}
		runJSONLListener(os.Args[2])
		return
	}
	if len(os.Args) != 4 {
		usage()
	}
	inputPath := os.Args[2]
	outputPath := os.Args[3]

	input, err := os.ReadFile(inputPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to read %s: %v\n", inputPath, err)
		os.Exit(1)
	}

	var output []byte
	switch mode {
	case "events":
		var rawEvents []json.RawMessage
		if err := json.Unmarshal(input, &rawEvents); err != nil {
			fmt.Fprintf(os.Stderr, "events mode requires an array payload: %v\n", err)
			os.Exit(1)
		}
		events := make([]*abxbus.BaseEvent, 0, len(rawEvents))
		for _, raw := range rawEvents {
			event, err := abxbus.BaseEventFromJSON(raw)
			if err != nil {
				fmt.Fprintf(os.Stderr, "failed to hydrate event JSON: %v\n", err)
				os.Exit(1)
			}
			events = append(events, event)
		}
		output, err = json.MarshalIndent(events, "", "  ")
	case "bus":
		bus, err := abxbus.EventBusFromJSON(input)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to hydrate event bus JSON: %v\n", err)
			os.Exit(1)
		}
		output, err = bus.ToJSON()
	default:
		usage()
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to serialize roundtrip payload: %v\n", err)
		os.Exit(1)
	}

	if err := os.WriteFile(outputPath, output, 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "failed to write %s: %v\n", outputPath, err)
		os.Exit(1)
	}
}

func runJSONLListener(configPath string) {
	input, err := os.ReadFile(configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to read listener config %s: %v\n", configPath, err)
		os.Exit(1)
	}
	var config jsonlListenerConfig
	if err := json.Unmarshal(input, &config); err != nil {
		fmt.Fprintf(os.Stderr, "failed to parse listener config: %v\n", err)
		os.Exit(1)
	}

	bridge := abxbus.NewJSONLEventBridge(config.Path, 0.05, "JSONLWorker")
	defer bridge.Close()

	received := make(chan struct{})
	receivedOnce := sync.Once{}
	bridge.On("*", "capture", func(ctx context.Context, event *abxbus.BaseEvent) (any, error) {
		data, err := json.Marshal(event)
		if err != nil {
			return nil, err
		}
		tempPath := config.OutputPath + ".tmp"
		if err := os.WriteFile(tempPath, data, 0o644); err != nil {
			return nil, err
		}
		if err := os.Rename(tempPath, config.OutputPath); err != nil {
			return nil, err
		}
		receivedOnce.Do(func() { close(received) })
		return nil, nil
	}, nil)
	if err := bridge.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "failed to start listener bridge: %v\n", err)
		os.Exit(1)
	}
	if err := os.WriteFile(config.ReadyPath, []byte("ready"), 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "failed to write ready marker: %v\n", err)
		os.Exit(1)
	}

	select {
	case <-received:
	case <-time.After(30 * time.Second):
		fmt.Fprintln(os.Stderr, "listener timed out waiting for bridge event")
		os.Exit(1)
	}
}
