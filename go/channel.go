package gatews

import (
	"crypto/hmac"
	"crypto/sha512"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

type SubscribeOptions struct {
	ID          int64 `json:"id"`
	IsReConnect bool  `json:"-"`
}

func (ws *WsService) Subscribe(channel string, payload []string) error {
	if (ws.conf.Key == "" || ws.conf.Secret == "") && authChannel[channel] {
		return newAuthEmptyErr()
	}

	msgCh, err := ws.messageChannel(channel)
	if err != nil {
		return err
	}

	return ws.newBaseChannel(channel, payload, msgCh, nil)
}

func (ws *WsService) SubscribeWithOption(channel string, payload any, op *SubscribeOptions) error {
	if (ws.conf.Key == "" || ws.conf.Secret == "") && authChannel[channel] {
		return newAuthEmptyErr()
	}

	msgCh, err := ws.messageChannel(channel)
	if err != nil {
		return err
	}

	return ws.newBaseChannel(channel, payload, msgCh, op)
}

func (ws *WsService) messageChannel(channel string) (chan *UpdateMsg, error) {
	if err := ws.Ctx.Err(); err != nil {
		return nil, err
	}
	msgCh, loaded := ws.msgChs.LoadOrStore(channel, make(chan *UpdateMsg, 1))
	ch := msgCh.(chan *UpdateMsg)
	if !loaded && !ws.startWorker(func() { ws.receiveCallMsg(channel, ch) }) {
		return nil, ws.Ctx.Err()
	}
	return ch, nil
}

func (ws *WsService) UnSubscribe(channel string, payload []string) error {
	return ws.baseSubscribe(UnSubscribe, channel, payload, nil)
}

func (ws *WsService) newBaseChannel(channel string, payload any, bch chan *UpdateMsg, op *SubscribeOptions) error {
	err := ws.baseSubscribe(Subscribe, channel, payload, op)
	if err != nil {
		return err
	}

	if _, ok := ws.msgChs.Load(channel); !ok {
		ws.msgChs.Store(channel, bch)
	}

	ws.readMsg()

	return nil
}

func (ws *WsService) baseSubscribe(event, channel string, payload any, op *SubscribeOptions) error {
	ts := time.Now().Unix()
	hash := hmac.New(sha512.New, []byte(ws.conf.Secret))
	hash.Write([]byte(fmt.Sprintf("channel=%s&event=%s&time=%d", channel, Subscribe, ts)))
	req := Request{
		Time:    ts,
		Channel: channel,
		Event:   event,
		Payload: payload,
		Auth: Auth{
			Method: AuthMethodApiKey,
			Key:    ws.conf.Key,
			Secret: hex.EncodeToString(hash.Sum(nil)),
		},
	}
	// options
	if op != nil {
		req.Id = &op.ID
	}

	byteReq, err := json.Marshal(req)
	if err != nil {
		ws.Logger.Printf("req Marshal err:%s", err.Error())
		return err
	}
	ws.mu.Lock()
	defer ws.mu.Unlock()

	if err := ws.Ctx.Err(); err != nil {
		return err
	}
	err = ws.GetConnection().WriteMessage(websocket.TextMessage, byteReq)
	if err != nil {
		ws.Logger.Printf("wsWrite [%s] err:%s", channel, err.Error())
		return err
	}

	if strings.HasSuffix(channel, "ping") {
		return nil
	}

	if v, ok := ws.conf.subscribeMsg.Load(channel); ok {
		if op != nil && op.IsReConnect {
			return nil
		}
		reqs := v.([]requestHistory)
		reqs = append(reqs, requestHistory{
			Channel: channel,
			Event:   event,
			Payload: payload,
		})
		ws.conf.subscribeMsg.Store(channel, reqs)
	} else {
		// avoid saving invalid subscribe msg
		if strings.HasSuffix(channel, ".ping") || strings.HasSuffix(channel, ".time") {
			return nil
		}

		ws.conf.subscribeMsg.Store(channel, []requestHistory{{
			Channel: channel,
			Event:   event,
			Payload: payload,
		}})
	}

	return nil
}

// readMsg only run once to read message
func (ws *WsService) readMsg() {
	ws.once.Do(func() {
		ws.startWorker(func() {
			defer ws.shutdown()

			for {
				select {
				case <-ws.Ctx.Done():
					ws.Logger.Printf("closing reader")
					return

				default:
					conn := ws.GetConnection()
					_, rawMsg, err := conn.ReadMessage()
					if err != nil {
						if ws.Ctx.Err() != nil {
							return
						}
						ws.Logger.Printf("websocket err: %s", err.Error())
						if e := ws.reconnect(conn); e != nil {
							ws.Logger.Printf("reconnect err:%s", e.Error())
							return
						}
						ws.Logger.Println("reconnect success, continue read message")
						continue
					}

					var msg UpdateMsg
					if err := json.Unmarshal(rawMsg, &msg); err != nil {
						continue
					}

					channel := msg.GetChannel()
					if channel == "" {
						ws.Logger.Printf("channel is empty in message %v", msg)
						return
					}

					if bch, ok := ws.msgChs.Load(channel); ok {
						select {
						case <-ws.Ctx.Done():
							return
						case bch.(chan *UpdateMsg) <- &msg:
						}
					}
				}
			}
		})
	})
}

type CallBack func(*UpdateMsg)

func NewCallBack(f func(*UpdateMsg)) func(*UpdateMsg) {
	return f
}

func (ws *WsService) SetCallBack(channel string, call CallBack) {
	if call == nil {
		return
	}
	ws.calls.Store(channel, call)
}

func (ws *WsService) receiveCallMsg(channel string, msgCh chan *UpdateMsg) {
	// avoid send closed channel error
	// defer close(msgCh)
	for {
		select {
		case <-ws.Ctx.Done():
			ws.Logger.Printf("received parent context exit")
			return
		case msg := <-msgCh:
			if ws.Ctx.Err() != nil {
				return
			}
			if call, ok := ws.calls.Load(channel); ok {
				call.(CallBack)(msg)
			}
		}
	}
}

func (ws *WsService) APIRequest(channel string, payload any, keyVals map[string]any) error {
	var err error
	ws.loginOnce.Do(func() {
		err = ws.login()
	})

	if err != nil {
		return err
	}

	if (ws.conf.Key == "" || ws.conf.Secret == "") && authChannel[channel] {
		return newAuthEmptyErr()
	}

	if _, err := ws.messageChannel(channel); err != nil {
		return err
	}

	ws.readMsg()

	return ws.apiRequest(channel, payload, keyVals)
}

func (ws *WsService) login() error {
	if ws.conf.Key == "" || ws.conf.Secret == "" {
		return newAuthEmptyErr()
	}
	channel := ChannelSpotLogin
	if ws.conf.App == "futures" {
		channel = ChannelFutureLogin
	}
	if _, err := ws.messageChannel(channel); err != nil {
		return err
	}

	ws.readMsg()

	return ws.apiRequest(channel, nil, nil)
}

func (ws *WsService) apiRequest(channel string, payload any, keyVals map[string]any) error {
	req := Request{
		Time:    time.Now().Unix(),
		Channel: channel,
		Event:   API,
		Payload: ws.generateAPIRequest(channel, payload, keyVals),
	}

	byteReq, err := json.Marshal(req)
	if err != nil {
		ws.Logger.Printf("req Marshal err:%s", err.Error())
		return err
	}
	ws.mu.Lock()
	defer ws.mu.Unlock()

	if err := ws.Ctx.Err(); err != nil {
		return err
	}
	return ws.GetConnection().WriteMessage(websocket.TextMessage, byteReq)
}

func (ws *WsService) generateAPIRequest(channel string, placeParam any, keyVals map[string]any) any {
	reqID := "req_id"
	gateChannelID := "T_channel_id"

	if v, ok := keyVals["req_id"]; ok {
		reqID, _ = v.(string)
	}

	if v, ok := keyVals["X-Gate-Channel-Id"]; ok {
		gateChannelID, _ = v.(string)
	}

	now := time.Now().Unix()

	reqParam, _ := json.Marshal(placeParam)

	message := fmt.Sprintf("api\n%s\n%s\n%d", channel, reqParam, now)

	return APIReq{
		ApiKey:    ws.conf.Key,
		Signature: calculateSignature(ws.conf.Secret, message),
		Timestamp: strconv.Itoa(int(now)),
		ReqId:     reqID,
		ReqHeader: json.RawMessage(fmt.Sprintf(`{"X-Gate-Channel-Id":"%s"}`, gateChannelID)),
		ReqParam:  reqParam,
	}
}

func calculateSignature(secret string, message string) string {
	h := hmac.New(sha512.New, []byte(secret))
	h.Write([]byte(message))
	return hex.EncodeToString(h.Sum(nil))
}
