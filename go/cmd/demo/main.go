package main

import (
	"flag"
	telemetry "github.com/anacrolix/telemetry/go"
	"log/slog"
	"net/url"
)

func main() {
	// Can switch to http for example to demonstrate POST.
	scheme := flag.String("scheme", "ws", "telemetry scheme")
	flag.Parse()
	telemetryWriter := telemetry.Writer{
		Url: &url.URL{
			Scheme: *scheme,
			// Should derive this from the default port somewhere.
			Host: "localhost:4318",
		},
	}
	jsonHandler := slog.NewJSONHandler(&telemetryWriter, nil)
	logger := slog.New(jsonHandler)
	logger.Info("hi mom")
	// TODO: This message could be written to the streams table.
	telemetryWriter.Close("i have to go now. goodbye!")
}
