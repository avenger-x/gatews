package gatews

import (
	"context"
	"errors"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

type retryLog struct {
	retried chan struct{}
}

func (w retryLog) Write(p []byte) (int, error) {
	if strings.Contains(string(p), "try again later") {
		select {
		case w.retried <- struct{}{}:
		default:
		}
	}
	return len(p), nil
}

func waitForExit(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("SDK did not exit promptly")
	}
}

func TestInitialConnectionCancellation(t *testing.T) {
	for _, phase := range []string{"before_dial", "handshake", "backoff"} {
		t.Run(phase, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			entered := make(chan struct{}, 1)
			retried := make(chan struct{}, 1)
			release := make(chan struct{})
			var requests int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				atomic.AddInt32(&requests, 1)
				entered <- struct{}{}
				if phase == "handshake" {
					<-release
					return
				}
				http.Error(w, "retry", http.StatusServiceUnavailable)
			}))
			defer server.Close()
			defer close(release)
			if phase == "before_dial" {
				cancel()
			}
			done := make(chan struct{})
			var result error
			go func() {
				defer close(done)
				ws, err := NewWsService(ctx, log.New(retryLog{retried}, "", 0), &ConnConf{
					URL: "ws" + strings.TrimPrefix(server.URL, "http"),
				})
				result = err
				if ws != nil {
					ws.Close()
				}
			}()
			if phase == "handshake" {
				waitForExit(t, entered)
			} else if phase == "backoff" {
				waitForExit(t, retried)
			}
			canceledAt := time.Now()
			cancel()
			waitForExit(t, done)
			if phase == "backoff" && time.Since(canceledAt) >= 250*time.Millisecond {
				t.Fatal("cancellation waited for retry backoff")
			}
			if !errors.Is(result, context.Canceled) {
				t.Fatalf("connection error = %v, want context.Canceled", result)
			}
			if phase == "before_dial" && atomic.LoadInt32(&requests) != 0 {
				t.Fatal("canceled context still initiated a handshake")
			}
		})
	}
}

func TestCancelAndConcurrentClose(t *testing.T) {
	for _, mode := range []string{"cancel", "close"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var requests, pings int32
			peerClosed := make(chan struct{})
			upgrader := websocket.Upgrader{}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				atomic.AddInt32(&requests, 1)
				conn, err := upgrader.Upgrade(w, r, nil)
				if err != nil {
					return
				}
				defer conn.Close()
				defer close(peerClosed)
				conn.SetPingHandler(func(data string) error {
					atomic.AddInt32(&pings, 1)
					return conn.WriteControl(websocket.PongMessage, []byte(data), time.Now().Add(time.Second))
				})
				for {
					if _, _, err := conn.ReadMessage(); err != nil {
						return
					}
				}
			}))
			defer server.Close()
			ws, err := NewWsService(ctx, log.New(io.Discard, "", 0), &ConnConf{
				URL: "ws" + strings.TrimPrefix(server.URL, "http"), PingInterval: "5ms",
			})
			if err != nil {
				t.Fatal(err)
			}
			defer ws.Close()
			if err := ws.Subscribe(ChannelSpotPublicTrade, []string{"BTC_USDT"}); err != nil {
				t.Fatal(err)
			}
			deadline := time.After(2 * time.Second)
			for atomic.LoadInt32(&pings) < 2 {
				select {
				case <-deadline:
					t.Fatal("heartbeat did not start")
				case <-time.After(time.Millisecond):
				}
			}
			if mode == "cancel" {
				cancel()
				// Parent cancellation alone must stop every worker and close the socket.
				done := make(chan struct{})
				go func() { ws.workers.Wait(); close(done) }()
				waitForExit(t, done)
			}
			var closers sync.WaitGroup
			for i := 0; i < 16; i++ {
				closers.Add(1)
				go func() {
					defer closers.Done()
					if err := ws.Close(); err != nil {
						t.Errorf("Close: %v", err)
					}
				}()
			}
			done := make(chan struct{})
			go func() { closers.Wait(); close(done) }()
			waitForExit(t, done)
			waitForExit(t, peerClosed)
			if ws.Status() != "disconnected" {
				t.Fatalf("status after Close = %s", ws.Status())
			}
			if err := ws.reconnect(); !errors.Is(err, context.Canceled) {
				t.Fatalf("reconnect after Close = %v", err)
			}
			if err := ws.Subscribe(ChannelSpotPublicTrade, nil); !errors.Is(err, context.Canceled) {
				t.Fatalf("subscribe after Close = %v", err)
			}
			before := atomic.LoadInt32(&pings)
			time.Sleep(20 * time.Millisecond)
			if atomic.LoadInt32(&pings) != before || atomic.LoadInt32(&requests) != 1 {
				t.Fatal("heartbeat or reconnect continued after Close")
			}
		})
	}
}

func TestExitDuringReconnect(t *testing.T) {
	for _, phase := range []string{"handshake", "backoff"} {
		for _, mode := range []string{"cancel", "close"} {
			t.Run(phase+"_"+mode, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				var requests int32
				peer := make(chan *websocket.Conn, 1)
				entered := make(chan struct{}, 1)
				retried := make(chan struct{}, 1)
				release := make(chan struct{})
				upgrader := websocket.Upgrader{}
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if atomic.AddInt32(&requests, 1) > 1 {
						entered <- struct{}{}
						if phase == "handshake" {
							<-release
							return
						}
						http.Error(w, "retry", http.StatusServiceUnavailable)
						return
					}
					conn, err := upgrader.Upgrade(w, r, nil)
					if err != nil {
						return
					}
					defer conn.Close()
					peer <- conn
					for {
						if _, _, err := conn.ReadMessage(); err != nil {
							return
						}
					}
				}))
				defer server.Close()
				defer close(release)
				ws, err := NewWsService(ctx, log.New(retryLog{retried}, "", 0), &ConnConf{
					URL: "ws" + strings.TrimPrefix(server.URL, "http"),
				})
				if err != nil {
					t.Fatal(err)
				}
				defer ws.Close()
				if err := ws.Subscribe(ChannelSpotPublicTrade, []string{"BTC_USDT"}); err != nil {
					t.Fatal(err)
				}
				(<-peer).Close()
				waitForExit(t, entered)
				if phase == "backoff" {
					waitForExit(t, retried)
				}
				stoppedAt := time.Now()
				if mode == "cancel" {
					cancel()
				}
				done := make(chan struct{})
				go func() {
					if mode == "close" {
						ws.Close()
					} else {
						ws.workers.Wait()
					}
					close(done)
				}()
				waitForExit(t, done)
				if phase == "backoff" && time.Since(stoppedAt) >= 250*time.Millisecond {
					t.Fatal("shutdown waited for reconnect backoff")
				}
				if ws.Status() != "disconnected" {
					t.Fatalf("status = %s", ws.Status())
				}
				if err := ws.reconnect(); !errors.Is(err, context.Canceled) {
					t.Fatalf("reconnect after exit = %v", err)
				}
				if got := atomic.LoadInt32(&requests); got != 2 {
					t.Fatalf("handshakes = %d, want 2", got)
				}
			})
		}
	}
}

func TestCloseWaitsForCallbackAndUnblocksDispatch(t *testing.T) {
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		if _, _, err := conn.ReadMessage(); err != nil {
			return
		}
		for i := 0; i < 4; i++ {
			if err := conn.WriteMessage(websocket.TextMessage, []byte(`{"channel":"spot.trades","event":"update","result":{}}`)); err != nil {
				return
			}
		}
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer server.Close()
	ws, err := NewWsService(nil, log.New(io.Discard, "", 0), &ConnConf{
		URL: "ws" + strings.TrimPrefix(server.URL, "http"),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ws.Close()
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	ws.SetCallBack(ChannelSpotPublicTrade, func(*UpdateMsg) {
		entered <- struct{}{}
		<-release
	})
	if err := ws.Subscribe(ChannelSpotPublicTrade, nil); err != nil {
		t.Fatal(err)
	}
	waitForExit(t, entered)
	done := make(chan struct{})
	go func() { ws.Close(); close(done) }()
	select {
	case <-done:
		t.Fatal("Close returned before the callback completed")
	case <-time.After(20 * time.Millisecond):
	}
	releaseOnce.Do(func() { close(release) })
	waitForExit(t, done)
}

func TestCloseDuringHeartbeatReconnect(t *testing.T) {
	var requests int32
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&requests, 1) > 1 {
			entered <- struct{}{}
			<-release
			return
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer server.Close()
	defer close(release)
	ws, err := NewWsService(nil, log.New(io.Discard, "", 0), &ConnConf{
		URL: "ws" + strings.TrimPrefix(server.URL, "http"), PingInterval: "5ms",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ws.Close()
	// Without a reader, pongs are not processed and the heartbeat reconnects.
	waitForExit(t, entered)
	done := make(chan struct{})
	go func() { ws.Close(); close(done) }()
	waitForExit(t, done)
	if ws.Status() != "disconnected" || atomic.LoadInt32(&requests) != 2 {
		t.Fatal("heartbeat reconnect did not stop")
	}
}

func TestCloseRacesWithSubscriptionAndReconnect(t *testing.T) {
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer server.Close()
	for i := 0; i < 20; i++ {
		ws, err := NewWsService(nil, log.New(io.Discard, "", 0), &ConnConf{
			URL: "ws" + strings.TrimPrefix(server.URL, "http"),
		})
		if err != nil {
			t.Fatal(err)
		}
		start := make(chan struct{})
		var operations sync.WaitGroup
		for j := 0; j < 8; j++ {
			operations.Add(1)
			go func() {
				defer operations.Done()
				<-start
				ws.Subscribe(ChannelSpotPublicTrade, []string{"BTC_USDT"})
				ws.Status()
			}()
		}
		operations.Add(2)
		go func() {
			defer operations.Done()
			<-start
			ws.reconnect()
		}()
		go func() {
			defer operations.Done()
			<-start
			ws.Close()
		}()
		close(start)
		done := make(chan struct{})
		go func() { operations.Wait(); close(done) }()
		waitForExit(t, done)
		if err := ws.Close(); err != nil || ws.Status() != "disconnected" {
			t.Fatalf("Close left a live service: %v, %s", err, ws.Status())
		}
	}
}
