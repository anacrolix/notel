package main

import (
	"flag"
	telemetry "github.com/anacrolix/telemetry/go"
	"log/slog"
	"net/url"
	"os"
)

func main() {
	err := mainErr()
	if err != nil {
		slog.Error("error in main", "err", err)
		os.Exit(1)
	}
}

func mainErr() (err error) {
	// Can switch to http for example to demonstrate POST.
	scheme := flag.String("scheme", "ws", "telemetry scheme")
	urlStr := flag.String("url", "", "telemetry url")
	flag.Parse()
	_url := &url.URL{
		Scheme: *scheme,
		// Should derive this from the default port somewhere.
		Host: "localhost:4318",
	}
	if *urlStr != "" {
		_url, err = url.Parse(*urlStr)
		if err != nil {
			return err
		}
	}
	telemetryWriter := telemetry.Writer{
		Url: _url,
	}
	jsonHandler := slog.NewJSONHandler(&telemetryWriter, nil)
	logger := slog.New(jsonHandler)
	logger.Info("hi mom")
	// TODO: This message could be written to the streams table.
	return telemetryWriter.Close("i have to go now. goodbye!")
}
