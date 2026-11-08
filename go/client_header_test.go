package gatews

import (
	"context"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestConnConfHeader(t *testing.T) {
	header := http.Header{"X-Gate-Size-Decimal": {"1"}}
	conf := NewConnConfFromOption(&ConfOptions{App: "futures", Header: header})
	if !reflect.DeepEqual(conf.Header, header) {
		t.Fatalf("options header = %v, want %v", conf.Header, header)
	}
	conf = applyOptionConf(getInitConnConf(), conf)
	if !reflect.DeepEqual(conf.Header, header) {
		t.Fatalf("merged header = %v, want %v", conf.Header, header)
	}

	defaults := getInitConnConf()
	defaults.Header = header
	conf = applyOptionConf(defaults, &ConnConf{})
	if !reflect.DeepEqual(conf.Header, header) {
		t.Fatalf("default header = %v, want %v", conf.Header, header)
	}
	custom := http.Header{"X-Gate-Size-Decimal": {"0"}}
	conf = applyOptionConf(defaults, &ConnConf{Header: custom})
	if !reflect.DeepEqual(conf.Header, custom) {
		t.Fatalf("custom header overwritten: %v", conf.Header)
	}
	conf = applyOptionConf(getInitConnConf(), NewConnConfFromOption(&ConfOptions{}))
	if conf.Header != nil {
		t.Fatalf("unset header = %v, want nil", conf.Header)
	}
}

func TestHandshakeHeaderInitialAndReconnect(t *testing.T) {
	for _, tc := range []struct {
		name   string
		header http.Header
		tls    bool
		direct bool
	}{
		{name: "custom", header: http.Header{
			"X-Gate-Size-Decimal": {"1"},
			"X-Custom-Values":     {"first", "second"},
		}},
		{name: "unset"},
		{name: "direct_config", direct: true, header: http.Header{"X-Gate-Size-Decimal": {"1"}}},
		{name: "tls_custom", tls: true, header: http.Header{"X-Gate-Size-Decimal": {"1"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			originalTLS := websocket.DefaultDialer.TLSClientConfig
			headers := make(chan http.Header, 2)
			upgrader := websocket.Upgrader{}
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, err := upgrader.Upgrade(w, r, nil)
				if err != nil {
					t.Errorf("upgrade: %v", err)
					return
				}
				defer conn.Close()
				headers <- r.Header.Clone()
				for {
					if _, _, err := conn.ReadMessage(); err != nil {
						return
					}
				}
			}))
			if tc.tls {
				server.StartTLS()
			} else {
				server.Start()
			}
			defer server.Close()

			url := "ws" + strings.TrimPrefix(server.URL, "http")
			conf := NewConnConfFromOption(&ConfOptions{
				URL: url, Header: tc.header, SkipTlsVerify: tc.tls, MaxRetryConn: -1,
			})
			if tc.direct {
				conf = &ConnConf{URL: url, Header: tc.header, MaxRetryConn: -1}
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			ws, err := NewWsService(ctx, log.New(io.Discard, "", 0), conf)
			if err != nil {
				t.Fatalf("initial connection: %v", err)
			}
			defer func() { ws.GetConnection().Close() }()

			for attempt := 0; attempt < 2; attempt++ {
				if attempt > 0 {
					old := ws.GetConnection()
					old.Close()
					if err := ws.reconnect(old); err != nil {
						t.Fatalf("reconnect: %v", err)
					}
					if ws.GetConnection() == old {
						t.Fatal("reconnect did not replace the connection")
					}
				}
				select {
				case got := <-headers:
					for name, want := range tc.header {
						if !reflect.DeepEqual(got.Values(name), want) {
							t.Errorf("handshake %d header %s = %v, want %v", attempt, name, got.Values(name), want)
						}
					}
					if tc.header == nil && got.Get("X-Gate-Size-Decimal") != "" {
						t.Errorf("handshake %d unexpectedly sent a custom header", attempt)
					}
				case <-time.After(5 * time.Second):
					t.Fatalf("timed out waiting for handshake %d", attempt)
				}
				if websocket.DefaultDialer.TLSClientConfig != originalTLS {
					t.Fatal("connection modified websocket.DefaultDialer")
				}
			}
		})
	}
}
