package telemetry

import (
	"context"
	"errors"
	"fmt"
	g "github.com/anacrolix/generics"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/anacrolix/chansync"
	"github.com/anacrolix/chansync/events"
	"nhooyr.io/websocket"
)

type Writer struct {
	// websocket and HTTP post are supported. Posting isn't very nice through Cloudflare.
	Url *url.URL
	// Logger for *this*. Probably don't want to loop it back to itself.
	Logger *slog.Logger
	// The time between reconnects to the Url.
	RetryInterval time.Duration
	// Override the library specific dial options, which for now is probably just some light
	// compression.
	WebsocketDialOptions *websocket.DialOptions
	// Extra headers sent after any protocol specific ones for transports that use HTTP. This is
	// intended for stream context, values that apply to all events streamed.
	StreamContextHttpHeader http.Header

	// Lazy init guard.
	init   sync.Once
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu sync.Mutex
	// This lets loggers not block.
	buf        chan []byte
	retry      [][]byte
	addPending chansync.BroadcastCond

	closed      chansync.SetOnce
	closeReason string
}

func (me *Writer) writerWaitCond() (
	stop bool, // Stop writing
	ready bool, // There are messages ready to go.
	newMessages events.Signaled, // An event for new messages.
) {
	me.mu.Lock()
	defer me.mu.Unlock()
	if me.ctx.Err() != nil {
		// Closed and hard limit.
		stop = true
		return
	}
	if len(me.buf) != 0 || len(me.retry) != 0 {
		ready = true
		return
	}
	if me.closed.IsSet() {
		// We're requested to stop and there's nothing to send.
		stop = true
		return
	}
	// Return the cond chan for new messages.
	newMessages = me.addPending.Signaled()
	return
}

// Returns true if there are messages pending, and false if we should stop writing.
func (me *Writer) writerWait() (ready bool) {
	for {
		stop, ready_, newMessages := me.writerWaitCond()
		if stop {
			return false
		}
		if ready_ {
			return true
		}
		select {
		case <-newMessages:
		case <-me.closed.Done():
		case <-me.ctx.Done():
		}
	}
}

func (me *Writer) writer() {
	defer me.wg.Done()
	for {
		if !me.writerWait() {
			return
		}
		me.Logger.DebugContext(me.ctx, "connecting")
		wait := func() bool {
			if strings.Contains(me.Url.Scheme, "ws") {
				return me.websocket()
			} else {
				me.streamPost()
				return true
			}
		}()
		if wait && me.closed.IsSet() {
			// We just failed, and have been closed. Don't try again.
			return
		}
		if wait {
			select {
			case <-time.After(me.RetryInterval):
			case <-me.closed.Done():
			}
		}
	}
}

// Waits a while to allow final messages to go through. Another method should be added to make this
// customizable. Nothing should be logged after calling this.
func (me *Writer) Close(reason string) error {
	me.lazyInit()
	me.closeReason = reason
	me.closed.Set()
	me.Logger.DebugContext(me.ctx, "waiting for writer")
	close(me.buf)
	go func() {
		time.Sleep(5 * time.Second)
		me.cancel()
	}()
	me.wg.Wait()
	return nil
}

func (me *Writer) websocketDialOptions() *websocket.DialOptions {
	var opts websocket.DialOptions
	if me.WebsocketDialOptions == nil {
		opts.CompressionMode = websocket.CompressionContextTakeover
	} else {
		opts = *me.WebsocketDialOptions
	}
	opts.HTTPHeader = opts.HTTPHeader.Clone()
	g.MakeMapIfNil(&opts.HTTPHeader)
	addHttpHeaders(opts.HTTPHeader, me.StreamContextHttpHeader)
	return &opts
}

// Add headers from `from` to `mut`. Note that `mut` should be non-nil.
func addHttpHeaders(mut, from http.Header) {
	for key, values := range from {
		for _, value := range values {
			mut.Add(key, value)
		}
	}
}

// wait is true if the caller should wait a while before retrying.
func (me *Writer) websocket() (wait bool) {
	conn, _, err := websocket.Dial(me.ctx, me.Url.String(), me.websocketDialOptions())
	if err != nil {
		me.Logger.ErrorContext(me.ctx, "error dialing websocket", "err", err)
		return true
	}
	defer func() {
		err := context.Cause(me.ctx)
		reason := me.closeReason
		if err != nil {
			reason = err.Error()
		}
		conn.Close(websocket.StatusNormalClosure, reason)
	}()
	ctx, cancel := context.WithCancel(me.ctx)
	go func() {
		err := me.payloadWriter(
			ctx,
			func(b []byte) error {
				err := conn.Write(ctx, websocket.MessageText, b)
				me.Logger.DebugContext(
					ctx, "wrote websocket text message",
					"bytes", b,
					"err", err,
				)
				return err
			},
		)
		if err != nil {
			me.Logger.ErrorContext(ctx, "payload writer failed", "err", err)
		}
		// Notify that we're not sending anymore.
		err = conn.Write(ctx, websocket.MessageBinary, nil)
		if err != nil {
			me.Logger.ErrorContext(ctx, "writing end of stream", "err", err)
		}
	}()
	err = me.websocketReader(me.ctx, conn)
	// Since we can't receive acks anymore, stop sending immediately.
	cancel()
	if err != nil {
		level := slog.LevelError
		if errors.Is(err, io.EOF) {
			level = slog.LevelDebug
		}
		me.Logger.Log(me.ctx, level, "reading from websocket", "err", err)
	}
	return false
}

func (me *Writer) websocketReader(ctx context.Context, conn *websocket.Conn) error {
	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			return err
		}
		me.Logger.DebugContext(
			ctx, "read from telemetry websocket",
			"message bytes", string(data))
	}
}

func (me *Writer) streamPost() {
	ctx, cancel := context.WithCancel(me.ctx)
	defer cancel()
	r, w := io.Pipe()
	go func() {
		defer w.Close()
		err := me.payloadWriter(ctx, func(b []byte) error {
			_, err := w.Write(b)
			return err
		})
		if err != nil {
			me.Logger.ErrorContext(ctx, "http post payload writer failed", "err", err)
		}
	}()
	defer r.Close()
	me.Logger.DebugContext(ctx, "starting post")
	// What's the content type for newline/ND/packed JSON streams?
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, me.Url.String(), r)
	if err != nil {
		me.Logger.ErrorContext(ctx, "error creating post request", "err", err)
		return
	}
	req.Header.Set("Content-Type", "application/jsonl")
	addHttpHeaders(req.Header, me.StreamContextHttpHeader)
	resp, err := http.DefaultClient.Do(req)
	me.Logger.DebugContext(ctx, "post returned")
	r.Close()
	if err != nil {
		me.Logger.ErrorContext(ctx, "error posting", "err", err)
		return
	}
	if resp.StatusCode != http.StatusOK {
		me.Logger.ErrorContext(
			ctx, "unexpected post response status code",
			"status code", resp.StatusCode)
	}
	resp.Body.Close()
}

func (me *Writer) payloadWriter(ctx context.Context, w func(b []byte) error) error {
	write := func(b []byte) (err error) {
		me.Logger.DebugContext(ctx, "writing payload", "len", len(b))
		err = w(b)
		if err != nil {
			me.Logger.DebugContext(ctx, "error writing payload", "err", err)
		}
		return
	}
	for {
		for len(me.retry) != 0 {
			err := write(me.retry[0])
			if err != nil {
				return fmt.Errorf("error writing retry: %w", err)
			}
			me.retry = me.retry[1:]
		}
		select {
		case b, ok := <-me.buf:
			if !ok {
				me.Logger.DebugContext(ctx, "buf closed")
				return nil
			}
			err := write(b)
			if err != nil {
				me.retry = append(me.retry, b)
				return err
			}
		case <-ctx.Done():
			return context.Cause(me.ctx)
		}
	}
}

func (me *Writer) lazyInit() {
	me.init.Do(func() {
		if me.Logger == nil {
			me.Logger = slog.Default()
		}
		me.buf = make(chan []byte, 1024)
		me.ctx, me.cancel = context.WithCancel(context.Background())
		if me.RetryInterval == 0 {
			me.RetryInterval = time.Minute
		}
		me.wg.Add(1)
		go me.writer()
	})
}

var errWriterClosed = errors.New("writer is closed")

func (me *Writer) Write(p []byte) (n int, err error) {
	me.lazyInit()
	select {
	// Wow, thanks for not reporting this with race detector, Go.
	case me.buf <- slices.Clone(p):
		me.addPending.Broadcast()
		return len(p), nil
	case <-me.closed.Done():
		return 0, errWriterClosed
	default:
		if me.closed.IsSet() {
			return 0, errWriterClosed
		}
		me.Logger.ErrorContext(me.ctx, "payload lost", "payload", p)
		return 0, errors.New("payload lost")
	}
}
