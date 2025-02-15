# What is this project?

The project contains simple components relating to telemetry. There is a client package for Go which provides a way to send telemetry data, and a server program to receive and store that data.

The implementation can be self-hosted. It doesn't specify a data format. The examples use JSON event payloads, websocket and HTTP POST as transports, and sqlite3/duckdb/json-files with duckdb view, or Postgres for storage. All of these choices are easily replaced, modified or extended.

Due to the simplicity of the transport protocol, it should be trivial to send telemetry from any language or web client interface (like cURL).

# What is telemetry?

Telemetry refers to the collection, transmission and storage of data generated by the operation of software.

# How do I use it?

Run the server

    cd rust-server
    RUST_LOG=debug cargo run -- sqlite

The server is now listening on port 4318.

Send some telemetry to the server. See the [Go client demo code](go/cmd/demo/main.go).

    cd go
    go run ./cmd/demo

Look in the `telemetry.db` file for your data. Note the below is for the sqlite backend, there are others:

    sqlite3 rust-server/telemetry.db
    sqlite> select json_pretty(headers) from streams;
    {
        "accept-encoding": "gzip",
        "connection": "Upgrade",
        "host": "localhost:4318",
        "sec-websocket-key": "E5qU710sXWmuAO5OLciWNw==",
        "sec-websocket-version": "13",
        "upgrade": "websocket",
        "user-agent": "Go-http-client/1.1"
    }
    sqlite> select json_pretty(payload) from events;
    {
        "time": "2024-07-03T15:16:55.563839+10:00",
        "level": "INFO",
        "msg": "hi mom"
    }

# What are the basics?

You have one or more applications that send a series of events in an arbitrary format called a stream. Something receives those events and records them with a reference to the stream from which they were received. There might be some metadata added to events and streams depending on your requirements.

Now that you have a growing store of your events, you can create queries, indexes and eviction policies of your choosing to do what you want with your data.

# What about metrics/logs/traces?

There is nothing special about any of these, they're all still emitted as a stream of data. You can implement whatever data schema you desire to enable different interpretations of your data.

For example metrics might be events that have longer lifetimes in your telemetry storage. You might combine metrics to save storage in the longer term. You might denote these with `'type': 'metric'` in your data format, and regularly delete your other types of shorter-lived telemetry. `delete from events where payload->'type' <> 'metric' and insert_datetime <= datetime('now, '-14 days')`.

Traces are hierarchically related events that can refer to each other. This can be easily done by having object fields in your data format like `'type': 'trace', 'trace': {'spanId': '420', 'parentSpanId': '69'}`. Note that languages like Rust for example have client libraries that allow implementing tracing output that doesn't require OpenTelemetry.

# What about OpenTelemetry?

OpenTelemetry is a framework backed by numerous Software-As-A-Service "Observability" providers. These companies are able to charge extraordinary amounts of money to handle data that mostly goes unused due to not knowing the shape or origin of any of it. OpenTelemetry has evolved from a specification that proposed tracing to a catch phrase that purports to handle metrics and logging too. The reality is that OpenTelemetry is very hard to understand, poorly or incompletely implemented in many languages, hard to configure, and very hard to debug.

Developers just want to ship the data from their application, to somewhere they can use it. If lots of applications are sending to one place, even better.

# What's the schema?

There is an events table which contains the individual telemetry events which are linked to the stream they were received on.

The streams table contains metadata for the stream, currently the HTTP headers received for the Websocket or HTTP POST that began the stream of events.

See the [example schema file](rust-server/proposed-schema.sql) for sqlite3.

# What are the provided transports?

The Websocket transport treats each text and binary message a distinct event (note no format is specified). If an empty binary message is received, the server hangs up. After a sequence of consecutive messages, the server replies with the count of messages stored from the stream so far.

The HTTP POST transport sends newline delimited JSON body. The rust-server streams them straight into the attached database. When the stream ends it replies with the number of events received. Note that it currently expects JSON because it needs to be able to separate events in the incoming stream. This could be relaxed to newlines, or interpreted from the Content-Type in the future.

The existing transports stream back the cumulative count of consecutive events received from the client and inserted into the store so that clients can implement retry or batching logic.

# What's next?

notel is ready for use now. If there are changes in the future, migration should not be complex due to the simplicity of the design, or you can just fork and do things your own way.

Here are some ideas for future development:

- Provide a way to stream events from the database. That way you can stream to the database then have a live view of your application. Most backends support committing immediately. The json-files backend has a commit trigger if you need to synchronize events/streams for consumption.

- Provide a [tracing subscriber](https://tracing.rs/tracing_subscriber/index.html) for Rust (for the [tracing crate](https://tracing.rs/tracing/)) that sends telemetry to the server in this implementation. Note that this handles conventional tracing and log concepts in one. This does not require OpenTelemetry! Find something similar for Go for tracing that isn't related to OpenTelemetry.

- Maybe add a Prometheus metrics endpoint scraper. Prometheus is already a well established and functional metrics system. It only suffers from the fact that something equally universal doesn't exist for logs and tracing, and that it's a pull model, which is difficult for short-lived and ephemeral applications.
