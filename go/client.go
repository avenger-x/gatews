package gatews

import (
	"context"
	"crypto/tls"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	mapset "github.com/deckarep/golang-set"
	"github.com/gorilla/websocket"
)

type status int

const (
	disconnected status = iota
	connected
	reconnecting
)

type WsService struct {
	lastPong    int64 // Keep 64-bit atomic state aligned on 32-bit platforms.
	mu          *sync.Mutex
	Logger      *log.Logger
	Ctx         context.Context
	Client      *websocket.Conn
	once        *sync.Once
	loginOnce   *sync.Once
	msgChs      *sync.Map // business chan
	calls       *sync.Map
	conf        *ConnConf
	status      status
	clientMu    *sync.Mutex
	reconnectMu sync.Mutex
	cancel      context.CancelFunc
	closeOnce   sync.Once
	workers     sync.WaitGroup
	closed      bool
}

// ConnConf default URL is spot websocket
type ConnConf struct {
	App              string
	subscribeMsg     *sync.Map
	URL              string
	Header           http.Header
	Key              string
	Secret           string
	MaxRetryConn     int
	SkipTlsVerify    bool
	ShowReconnectMsg bool
	PingInterval     string
}

type ConfOptions struct {
	App              string
	URL              string
	Header           http.Header
	Key              string
	Secret           string
	MaxRetryConn     int
	SkipTlsVerify    bool
	ShowReconnectMsg bool
	PingInterval     string
}

func NewWsService(ctx context.Context, logger *log.Logger, conf *ConnConf) (*WsService, error) {
	if logger == nil {
		logger = log.New(os.Stdout, "", log.LstdFlags|log.Lshortfile)
	}
	if ctx == nil {
		ctx = context.Background()
	}

	defaultConf := getInitConnConf()
	if conf != nil {
		conf = applyOptionConf(defaultConf, conf)
	} else {
		conf = defaultConf
	}

	ctx, cancel := context.WithCancel(ctx)
	conn, err := connect(ctx, logger, conf)
	if err != nil {
		cancel()
		return nil, err
	}
	ws := &WsService{
		mu:        new(sync.Mutex),
		conf:      conf,
		Logger:    logger,
		Ctx:       ctx,
		Client:    conn,
		calls:     new(sync.Map),
		msgChs:    new(sync.Map),
		once:      new(sync.Once),
		loginOnce: new(sync.Once),
		status:    connected,
		clientMu:  new(sync.Mutex),
		cancel:    cancel,
	}
	ws.prepareConnection(conn)
	ws.workers.Add(2)
	go func() {
		defer ws.workers.Done()
		<-ctx.Done()
		ws.shutdown()
	}()
	go func() {
		defer ws.workers.Done()
		ws.keepAlive()
	}()

	return ws, nil
}

func connect(ctx context.Context, logger *log.Logger, conf *ConnConf) (*websocket.Conn, error) {
	for retry := 0; ; retry++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		conn, err := dialConnection(ctx, conf)
		if err == nil {
			return conn, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if retry >= conf.MaxRetryConn {
			return nil, err
		}
		logger.Printf("failed to connect to server for the %d time, try again later", retry+1)
		timer := time.NewTimer(time.Duration(retry+1) * 500 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

func dialConnection(ctx context.Context, conf *ConnConf) (*websocket.Conn, error) {
	dialer := *websocket.DefaultDialer
	if conf.SkipTlsVerify {
		dialer.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}
	netDial := dialer.NetDialContext
	if netDial == nil {
		if dialer.NetDial != nil {
			netDial = func(_ context.Context, network, addr string) (net.Conn, error) {
				return dialer.NetDial(network, addr)
			}
		} else {
			netDial = (&net.Dialer{}).DialContext
		}
	}

	// Gorilla v1.4.2 does not interrupt an in-flight handshake on cancellation.
	pending := make(chan net.Conn, 1)
	finished := make(chan struct{})
	watchDone := make(chan struct{})
	go func() {
		defer close(watchDone)
		select {
		case conn := <-pending:
			select {
			case <-ctx.Done():
				conn.Close()
			case <-finished:
			}
		case <-finished:
		}
	}()
	dialer.NetDialContext = func(dialCtx context.Context, network, addr string) (net.Conn, error) {
		conn, err := netDial(dialCtx, network, addr)
		if err == nil {
			pending <- conn
		}
		return conn, err
	}
	conn, _, err := dialer.DialContext(ctx, conf.URL, conf.Header)
	close(finished)
	<-watchDone
	if ctx.Err() != nil {
		if conn != nil {
			conn.Close()
		}
		return nil, ctx.Err()
	}
	return conn, err
}

// Close cancels all work and waits for SDK goroutines to exit. It is safe to call
// concurrently or repeatedly. Callbacks must return before Close can complete;
// do not call Close synchronously from a callback.
func (ws *WsService) Close() error {
	ws.shutdown()
	ws.workers.Wait()
	return nil
}

func (ws *WsService) shutdown() {
	ws.closeOnce.Do(func() {
		ws.cancel()
		ws.clientMu.Lock()
		ws.closed = true
		ws.status = disconnected
		conn := ws.Client
		ws.clientMu.Unlock()
		if conn != nil {
			conn.Close()
		}
	})
}

func (ws *WsService) beginWork() bool {
	ws.clientMu.Lock()
	defer ws.clientMu.Unlock()
	if ws.closed || ws.Ctx.Err() != nil {
		return false
	}
	ws.workers.Add(1)
	return true
}

func (ws *WsService) startWorker(f func()) bool {
	if !ws.beginWork() {
		return false
	}
	go func() {
		defer ws.workers.Done()
		f()
	}()
	return true
}

func (ws *WsService) prepareConnection(conn *websocket.Conn) {
	atomic.StoreInt64(&ws.lastPong, time.Now().UnixNano())
	conn.SetPongHandler(func(string) error {
		atomic.StoreInt64(&ws.lastPong, time.Now().UnixNano())
		return nil
	})
}

func getInitConnConf() *ConnConf {
	return &ConnConf{
		App:              "spot",
		subscribeMsg:     new(sync.Map),
		MaxRetryConn:     MaxRetryConn,
		Key:              "",
		Secret:           "",
		URL:              BaseUrl,
		SkipTlsVerify:    false,
		ShowReconnectMsg: true,
		PingInterval:     DefaultPingInterval,
	}
}

func applyOptionConf(defaultConf, userConf *ConnConf) *ConnConf {
	if userConf.App == "" {
		userConf.App = defaultConf.App
	}

	if userConf.URL == "" {
		userConf.URL = defaultConf.URL
	}
	if userConf.Header == nil {
		userConf.Header = defaultConf.Header
	}
	if userConf.subscribeMsg == nil {
		userConf.subscribeMsg = new(sync.Map)
	}

	if userConf.MaxRetryConn == 0 {
		userConf.MaxRetryConn = defaultConf.MaxRetryConn
	}

	if userConf.PingInterval == "" {
		userConf.PingInterval = defaultConf.PingInterval
	}

	return userConf
}

// NewConnConfFromOption conf from options, recommend using this
func NewConnConfFromOption(op *ConfOptions) *ConnConf {
	if op.URL == "" {
		op.URL = BaseUrl
	}
	if op.MaxRetryConn == 0 {
		op.MaxRetryConn = MaxRetryConn
	}
	return &ConnConf{
		App:              op.App,
		subscribeMsg:     new(sync.Map),
		MaxRetryConn:     op.MaxRetryConn,
		Key:              op.Key,
		Secret:           op.Secret,
		URL:              op.URL,
		Header:           op.Header,
		SkipTlsVerify:    op.SkipTlsVerify,
		ShowReconnectMsg: op.ShowReconnectMsg,
		PingInterval:     op.PingInterval,
	}
}

func (ws *WsService) GetConnConf() *ConnConf {
	return ws.conf
}

func (ws *WsService) reconnect(oldConn ...*websocket.Conn) error {
	if !ws.beginWork() {
		return ws.Ctx.Err()
	}
	defer ws.workers.Done()
	ws.reconnectMu.Lock()
	defer ws.reconnectMu.Unlock()
	if err := ws.Ctx.Err(); err != nil {
		return err
	}
	ws.clientMu.Lock()
	if ws.Ctx.Err() != nil || ws.closed {
		ws.clientMu.Unlock()
		return ws.Ctx.Err()
	}

	// Another goroutine may have already replaced the connection that failed.
	if len(oldConn) > 0 && oldConn[0] != nil && ws.Client != oldConn[0] {
		ws.clientMu.Unlock()
		return nil
	}
	previous := ws.Client
	ws.status = reconnecting
	ws.clientMu.Unlock()
	if previous != nil {
		previous.Close()
	}

	conn, err := connect(ws.Ctx, ws.Logger, ws.conf)
	if err != nil {
		ws.clientMu.Lock()
		ws.status = disconnected
		ws.clientMu.Unlock()
		return err
	}
	ws.prepareConnection(conn)
	ws.clientMu.Lock()
	if ws.Ctx.Err() != nil || ws.closed {
		ws.clientMu.Unlock()
		conn.Close()
		return ws.Ctx.Err()
	}
	ws.Client = conn
	ws.status = connected
	ws.clientMu.Unlock()

	// resubscribe after reconnect
	ws.conf.subscribeMsg.Range(func(key, value interface{}) bool {
		if ws.Ctx.Err() != nil {
			return false
		}
		// key is channel, value is []requestHistory
		if _, ok := value.([]requestHistory); ok {
			for _, req := range value.([]requestHistory) {
				op := SubscribeOptions{IsReConnect: true}
				if req.op != nil {
					op.ID = req.op.ID
				}
				if err := ws.baseSubscribe(req.Event, req.Channel, req.Payload, &op); err != nil {
					ws.Logger.Printf("after reconnect, subscribe channel[%s] err:%s", key.(string), err.Error())
				} else {
					if ws.conf.ShowReconnectMsg {
						ws.Logger.Printf("reconnect channel[%s] with payload[%v] success", key.(string), req.Payload)
					}
				}
			}
		}
		return true
	})

	return ws.Ctx.Err()
}

func (ws *WsService) SetKey(key string) {
	ws.conf.Key = key
}

func (ws *WsService) GetKey() string {
	return ws.conf.Key
}

func (ws *WsService) SetSecret(secret string) {
	ws.conf.Secret = secret
}

func (ws *WsService) GetSecret() string {
	return ws.conf.Secret
}

func (ws *WsService) SetMaxRetryConn(max int) {
	ws.conf.MaxRetryConn = max
}

func (ws *WsService) GetMaxRetryConn() int {
	return ws.conf.MaxRetryConn
}

func (ws *WsService) GetChannelMarkets(channel string) []string {
	var markets []string
	set := mapset.NewSet()
	if v, ok := ws.conf.subscribeMsg.Load(channel); ok {
		for _, req := range v.([]requestHistory) {
			payloads, ok := req.Payload.([]string)
			if !ok {
				continue
			}

			if req.Event == Subscribe {
				for _, pl := range payloads {
					if strings.Contains(pl, "_") {
						set.Add(pl)
					}
				}
			} else {
				for _, pl := range payloads {
					if strings.Contains(pl, "_") {
						set.Remove(pl)
					}
				}
			}
		}

		for _, v := range set.ToSlice() {
			markets = append(markets, v.(string))
		}
		return markets
	}
	return markets
}

func (ws *WsService) GetChannels() []string {
	var channels []string
	ws.calls.Range(func(key, value interface{}) bool {
		channels = append(channels, key.(string))
		return true
	})
	return channels
}

func (ws *WsService) GetConnection() *websocket.Conn {
	ws.clientMu.Lock()
	defer ws.clientMu.Unlock()

	return ws.Client
}

func (ws *WsService) keepAlive() {
	interval, err := time.ParseDuration(ws.conf.PingInterval)
	if err != nil || interval <= 0 {
		interval = 10 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ws.Ctx.Done():
			return
		case <-ticker.C:
		}
		if ws.Ctx.Err() != nil {
			return
		}
		conn := ws.GetConnection()
		if err := conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(10*time.Second)); err != nil {
			if ws.Ctx.Err() != nil {
				return
			}
			ws.Logger.Printf("send ping err:%s", err.Error())
		}
		if time.Since(time.Unix(0, atomic.LoadInt64(&ws.lastPong))) > 3*interval {
			if err := ws.reconnect(conn); err != nil {
				if ws.Ctx.Err() != nil {
					return
				}
				ws.Logger.Printf("reconnect err:%s", err.Error())
			}
		}
	}
}

var statusString = map[status]string{
	disconnected: "disconnected",
	connected:    "connected",
	reconnecting: "reconnecting",
}

func (ws *WsService) Status() string {
	ws.clientMu.Lock()
	defer ws.clientMu.Unlock()
	return statusString[ws.status]
}
