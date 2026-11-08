# Gate WebSocket Go SDK

`gatews` provides Gate WebSocket V4 Go implementation, including all channels defined in spot(new) WebSocket.

Features:

1. Fully asynchronous
2. Reconnect on connection to server lost, and resubscribe on connection recovered
3. Support connecting to multiple websocket servers
4. Highly configurable

## Installation

```shell
go get github.com/gateio/gatews/go
```

## Getting started

```golang
package main

import (
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	gate "github.com/gateio/gatews/go"
)

func main() {
	// create WsService with ConnConf, this is recommended, key and secret will be needed by some channels
	// ctx and logger could be nil, they'll be initialized by default
	ws, err := gate.NewWsService(nil, nil, gate.NewConnConfFromOption(&gate.ConfOptions{
		Key:           "YOUR_API_KEY",
		Secret:        "YOUR_API_SECRET",
		MaxRetryConn:  10, // default value is math.MaxInt64, set it when needs
		SkipTlsVerify: false,
	}))
	// we can also do nothing to get a WsService, all parameters will be initialized by default and default url is spot
	// but some channels need key and secret for auth, we can also use set function to set key and secret
	// ws, err := gate.NewWsService(nil, nil, nil)
	// ws.SetKey("YOUR_API_KEY")
	// ws.SetSecret("YOUR_API_SECRET")
	if err != nil {
		log.Printf("NewWsService err:%s", err.Error())
		return
	}

	// checkout connection status when needs
	go func() {
		ticker := time.NewTicker(time.Second)
		for {
			<-ticker.C
			log.Println("connetion status:", ws.Status())
		}
	}()

	// create callback functions for receive messages
	callOrder := gate.NewCallBack(func(msg *gate.UpdateMsg) {
		// parse the message to struct we need
		var order []gate.SpotOrderMsg
		if err := json.Unmarshal(msg.Result, &order); err != nil {
			log.Printf("order Unmarshal err:%s", err.Error())
		}
		log.Printf("%+v", order)
	})

	callTrade := gate.NewCallBack(func(msg *gate.UpdateMsg) {
		var trade gate.SpotTradeMsg
		if err := json.Unmarshal(msg.Result, &trade); err != nil {
			log.Printf("trade Unmarshal err:%s", err.Error())
		}
		log.Printf("%+v", trade)
	})

	// first, we need set callback function
	ws.SetCallBack(gate.ChannelSpotOrder, callOrder)
	ws.SetCallBack(gate.ChannelSpotPublicTrade, callTrade)
	// second, after set callback function, subscribe to any channel you are interested into
	if err := ws.Subscribe(gate.ChannelSpotPublicTrade, []string{"BTC_USDT"}); err != nil {
		log.Printf("Subscribe err:%s", err.Error())
		return
	}
	if err := ws.Subscribe(gate.ChannelSpotBookTicker, []string{"BTC_USDT"}); err != nil {
		log.Printf("Subscribe err:%s", err.Error())
		return
	}

	// example for maintaining local order book
	// LocalOrderBook(context.Background(), ws, []string{"BTC_USDT"})

	ch := make(chan os.Signal)
	signal.Ignore(syscall.SIGPIPE, syscall.SIGALRM)
	signal.Notify(ch, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT, syscall.SIGABRT, syscall.SIGKILL)
	<-ch
}
```

## Example

We provide some demo applications in the [examples](_examples) directory, which can be run directly.

## Custom Handshake Headers

Headers configured through `ConfOptions` or `ConnConf` are sent on both the
initial connection and every reconnect. Leaving `Header` nil keeps the default
handshake behavior.

```go
conf := gate.NewConnConfFromOption(&gate.ConfOptions{
	App: "futures",
	URL: gate.FuturesUsdtUrl,
	Header: http.Header{
		"X-Gate-Size-Decimal": []string{"1"},
	},
})
ws, err := gate.NewWsService(ctx, logger, conf)
```

Import `net/http` to construct the headers.

## Shutdown

Initial connections, reconnects, and retry waits honor the context passed to
`NewWsService`. Canceling it closes the active connection and stops heartbeats,
message reads, and callback workers. `Close()` also cancels the service and waits
for its background work to finish; concurrent or repeated calls are safe. A
closed service cannot reconnect or create new subscriptions.

```go
ctx, cancel := context.WithCancel(context.Background())
defer cancel()
ws, err := gate.NewWsService(ctx, logger, conf)
if err != nil {
	return err
}
defer ws.Close()
```

Callbacks run synchronously on SDK workers. They must return before `Close()`
can finish, so call `Close()` from outside a callback (or launch it in a separate
goroutine). `PingInterval` controls the heartbeat interval, defaulting to `10s`.

## Tests

Run `go test ./...` from this directory for local tests. The existing live
exchange tests require `go test -tags integration ./...`; some wait for a signal
or stream data for an extended period and are intended to be run manually.

## ChangeLog

[changelog](changelog.md)
