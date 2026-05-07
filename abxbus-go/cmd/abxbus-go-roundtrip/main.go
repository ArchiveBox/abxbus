package main

import (
	"encoding/json"
	"fmt"
	"os"

	abxbus "github.com/ArchiveBox/abxbus/abxbus-go"
)

func usage() {
	fmt.Fprintln(os.Stderr, "usage: abxbus-go-roundtrip <events|bus> <input.json> <output.json>")
	os.Exit(2)
}

func main() {
	if len(os.Args) != 4 {
		usage()
	}

	mode := os.Args[1]
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
