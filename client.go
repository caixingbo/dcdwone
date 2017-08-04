// Copyright 2017 The dcdwone Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"log"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
	"encoding/json"
	"encoding/base64"
	"strings"
	"strconv"
)

const (
	// Time allowed to write a message to the peer.
	writeWait = 10 * time.Second

	// Time allowed to read the next pong message from the peer.
	pongWait = 60 * time.Second

	// Send pings to peer with this period. Must be less than pongWait.
	pingPeriod = (pongWait * 9) / 10

	// Maximum message size allowed from peer.
	maxMessageSize = 2048

)

var (
	newline = []byte{'\n'}
	//space   = []byte{' '}
	token = base64.StdEncoding.EncodeToString([]byte("2017dwdatacenter"))
)

//var upgrader = websocket.Upgrader{
//	ReadBufferSize:  1024,
//	WriteBufferSize: 1024,
//}

var upgrader = websocket.Upgrader{
	ReadBufferSize:  2048,
	WriteBufferSize: 2048,
	CheckOrigin: func(r *http.Request) bool { return true }, // ignore original
	//CheckOrigin: nil,
}

// Client is a middleman between the websocket connection and the hub.
type Client struct {
	hub *Hub  //客户端管理控制类
	storage *Storage //数据持久化存储类
	conn *websocket.Conn //webSocket连接句柄
	clientType int //client类型，MasterType BrowserType
	order string //Master主机编号
	deviceStatus []int   //主机设备状态
	sendStatus int //报告主机状态
	send chan []byte //Buffered channel发送socket.
	exchange chan []byte //Buffered channel传输到BrowserClient.
	browsers map[*Client]bool //Browser终端句柄映射
	register chan *Client //Register requests from hub.
	unregister chan *Client //UnRegister requests from hub.
	msgDict map[string]float32
	timeDict map[string]int64
	warnDic map[string][]string
	log *log.Logger
}

func newClient(hub *Hub, storage *Storage, conn *websocket.Conn,log *log.Logger) *Client {
	return &Client{
		hub:       hub,
		conn:      conn,
		storage:   storage,
		clientType:0,
		order:     "",
		sendStatus:cancleWarning,
		deviceStatus:make([]int,10),
		browsers:  make(map[*Client]bool),
		send:      make(chan []byte, 256),
		exchange:  make(chan []byte, 256),
		register:  make(chan *Client),
		unregister:make(chan *Client),
		msgDict:   make(map[string]float32),
		timeDict: make(map[string]int64),
		log : log,
	}
}

// readPump pumps messages from the websocket connection to the hub.
//
// The application runs readPump in a per-connection goroutine. The application
// ensures that there is at most one reader on a connection by executing all
// reads from this goroutine.
func (c *Client) readPump() {
	defer func() {
		c.hub.unregister <- c
		c.conn.Close()
		if c.clientType == MasterType {
			c.log.Printf("readPump defer, close master order: %v",c.order)
		}else if c.clientType == BrowserType {
			c.log.Printf("readPump defer, close browser id: %v",c.order)
		}else {
			c.log.Printf("readPump defer, close unknown Client")
		}

	}()
	c.conn.SetReadLimit(maxMessageSize)
	c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error { c.conn.SetReadDeadline(time.Now().Add(pongWait)); return nil })
	for {
		_, message, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway) {
				c.log.Printf("IsUnexpectedCloseError: %v", err)
			}
			c.log.Printf("c.conn.ReadMessage: %v", err)
			break
		}

		if PrintDebug == 1 ||  PrintDebug == 9 {log.Println("Receive:",string(message))}
		// Json decode 未知结构的MsgID
		js := make(map[string]interface{})
		err = json.Unmarshal(message, &js)
		if err != nil {
			c.log.Printf("error: %v", err)
			break
		}else{
			var msgid int
			v1 := js["MsgID"]
			//c.log.Printf("MsgID:%#v\n", js)
			switch  v2:= v1.(type) {
			case string:
				msgid,_ = strconv.Atoi(v2)
			}

			if  msgid == masterLogin {
				//主机终端登录
				var mlogin MasterLogin
				err := json.Unmarshal(message, &mlogin)
				if err != nil {
					c.log.Printf("master login error: %v", err)
					break
				}

				token := base64.StdEncoding.EncodeToString([]byte(token+
					base64.StdEncoding.EncodeToString([]byte(mlogin.Order))))
				if strings.Compare(token,mlogin.Token) != 0 {
					c.log.Printf("C_right: %v",token)
					c.log.Printf("Receive: %v",mlogin.Token)
					break
				}

				errCode := 0
				if len(mlogin.Order) != 6 {
					errCode = 1011
				}else if c.clientType > 0 {
					errCode = 1101
				}else {
					for order := range c.hub.orders {
						if strings.Compare(mlogin.Order, order) == 0 {
							errCode = 1102
							c.log.Printf("master login err exist order: %v",order)
						}
					}
					if errCode == 0 {
						c.clientType = MasterType
						c.order = mlogin.Order
						c.hub.register <- c
						c.sendStorageMaster(c.order,mlogin.Mac,1)
						c.log.Printf("master login order: %v",c.order)
					}
				}
				c.send <- []byte("{\"MsgID\":\"" + strconv.Itoa(msgid+1) +
					"\",\"Err\":" + strconv.Itoa(errCode) + "}")

			}else if  msgid == browserLogin {
				//浏览器终端登录
				var blogin BrowserLogin
				err := json.Unmarshal(message, &blogin)
				if err != nil {
					c.log.Printf("brower login error: %v", err)
					break
				}

				token := base64.StdEncoding.EncodeToString([]byte(token+
					base64.StdEncoding.EncodeToString([]byte(blogin.Name))))
				if strings.Compare(token,blogin.Token) != 0 {
					c.log.Printf("B_right: %v",token)
					c.log.Printf("Receive: %v",blogin.Token)
					break
				}

				errCode := 0
				if c.clientType > 0 {
					errCode = 1101
				}

				if errCode == 0 {
					c.clientType = BrowserType
					c.order = strconv.FormatInt(time.Now().UnixNano(),10)
					c.hub.register <- c
					c.log.Printf("browser login name:%v id:%v",blogin.Name,c.order)
				}

				c.send <- []byte("{\"MsgID\":\"" + strconv.Itoa(msgid+1) +
					"\",\"Err\":" + strconv.Itoa(errCode) + "}")

			}else {
				if  c.clientType == 0 {
					//没有登录发送命令
					c.log.Printf("not login server error, msgid:%d",msgid)
					break
				}

				if msgid == takeOrder {
					//浏览器请求主机数据
					var torder TakeOrder
					err := json.Unmarshal(message, &torder)
					if err != nil {
						c.log.Printf("take order error: %v", err)
						break
					}
					errCode := 0
					if msgid / 10000 != c.clientType {
						errCode = 1110
					}else {
						result := &DoCommand{
							takeOrder,
							c.order,
							torder.Orders,
						}
						b, _ := json.Marshal(result)
						c.hub.broadcast <- b
					}
					c.send <- []byte("{\"MsgID\":\"" + strconv.Itoa(msgid+1) +
						"\",\"Err\":" + strconv.Itoa(errCode) + "}")

				}else if msgid == cancleOrder {
					//浏览器取消主机数据
					var dorder DeleteOrder
					err := json.Unmarshal(message, &dorder)
					if err != nil {
						c.log.Printf("cancle order error: %v", err)
						break
					}

					errCode := 0
					if msgid / 10000 != c.clientType {
						errCode = 1110
					}else {
						result := &DoCommand{
							cancleOrder,
							c.order,
							dorder.Orders,
						}
						b, _ := json.Marshal(result)
						c.hub.broadcast <- b
					}
					c.send <- []byte("{\"MsgID\":\"" + strconv.Itoa(msgid+1) +
						"\",\"Err\":" + strconv.Itoa(errCode) + "}")

				}else if msgid == openSuo {
					//浏览器远程开门
					var opensuo OpenSuo
					err := json.Unmarshal(message, &opensuo)
					if err != nil {
						c.log.Printf("open suo error: %v", err)
						break
					}
					errCode := 0
					if msgid / 10000 != c.clientType {
						errCode = 1110
					}else {
						result := &DoCommand{
							openSuo,
							c.order,
							opensuo.Orders,
						}
						b, _ := json.Marshal(result)
						c.hub.broadcast <- b
					}
					c.send <- []byte("{\"MsgID\":\"" + strconv.Itoa(msgid+1) +
						"\",\"Err\":" + strconv.Itoa(errCode) + "}")

				}else if msgid == checkOnline {
					//获取在线主机order
					var online CheckOnline
					err := json.Unmarshal(message, &online)
					if err != nil {
						c.log.Printf("check online error: %v", err)
						break
					}

					var masters []Master
					for zid := range  c.storage.zidmap {
						order := strconv.Itoa(zid)
						link := 2
						if online ,ok := c.hub.status[order]; ok{
							link = online
						}
						m := &Master{
							Order:order,
							IsAlarm:link,
						}
						masters = append(masters,*m)
					}


					if msgid / 10000 != c.clientType {
						errCode := 1110
						c.send <- []byte("{\"MsgID\":\"" + strconv.Itoa(msgid+1) +
							"\",\"Err\":" + strconv.Itoa(errCode) + "}")

					}else {
						result := &OnlineMaster{
							strconv.Itoa(msgid+1),
							0,
							masters,
						}
						b, _ := json.Marshal(result)
						c.send <- b
					}

				}else if msgid == warnInfo && msgid / 10000 == c.clientType {
					//主机告警信息
					var warning Warning
					err := json.Unmarshal(message, &warning)
					if err != nil {
						c.log.Printf("warnInfo error: %v", err)
						break
					}else {
						name := warning.Name
						addr := warning.Address
						value:= warning.Value
						datetime:=warning.DateTime
						errorInfo := warning.WarnInfo
						c.sendStorageWarn(c.order,name,addr,value,errorInfo,datetime)
						if PrintDebug == 5 ||  PrintDebug == 9  {
							log.Println("WarnTable",c.order,name,addr,value,errorInfo,datetime)
						}

					}
				}else if msgid == activeMaster && msgid / 10000 == c.clientType {
					//主机心跳检测
					errCode := 0
					c.send <- []byte("{\"MsgID\":\"" + strconv.Itoa(msgid+1) +
						"\",\"Err\":" + strconv.Itoa(errCode) + "}")
					if PrintDebug == 8 ||  PrintDebug == 9 {
						log.Printf("active msg send %v",c.order)
					}
				}else  {
					//主机采集信息数据，定时入数据库，用户终端联线实时转发
					errCode := 0
					if msgid == wsysInfo && msgid / 10000 == c.clientType{
						//温湿度，水浸烟感信息
						var wsysinfo WsysInfo
						err := json.Unmarshal(message, &wsysinfo)
						if err != nil {
							c.log.Printf("wsysInfo error: %v", err)
							break
						}
						c.deviceStatus[0] = cancleWarning
						c.deviceStatus[1] = cancleWarning
						for k := range wsysinfo.WSdu {
							v := wsysinfo.WSdu[k]

							if v.IsConnect == 0 && v.IsShield == 0 {
								c.deviceStatus[0] = masterWarning
								c.deviceStatus[1] = masterWarning
							}

							if v.IsConnect == 1 && v.IsShield == 0 {
								if ok := c.checkSendDict(
									msgid, "wendu"+strconv.Itoa(k), v.Tvalue); ok {
									c.sendStorageData(WDTable, c.order, k, v.Tvalue)
								}
								if ok := c.checkSendDict(
									msgid, "shidu"+strconv.Itoa(k), v.Hvalue); ok {
									c.sendStorageData(SDTable, c.order, k, v.Hvalue)
								}
								if v.TAlarm == 1 {
									c.deviceStatus[0] = masterWarning
								}
								if v.HAlarm == 1 {
									c.deviceStatus[1] = masterWarning
								}
								if PrintDebug == 4 || PrintDebug == 9 {
									log.Println("SDTable", c.order, k, float32(v.Hvalue))
									log.Println("WDTable", c.order, k, float32(v.Tvalue))
								}
							}
						}
						c.deviceStatus[2] = cancleWarning
						for k := range wsysinfo.Shuijin {
							v := wsysinfo.Shuijin[k]

							if v.IsConnect == 0 && v.IsShield == 0 {
								c.deviceStatus[2] = masterWarning
							}

							if v.IsConnect == 1 && v.IsShield == 0 {
								if ok := c.checkSendDict(
									msgid, "shuijin"+strconv.Itoa(k), float32(v.Value)); ok {
									c.sendStorageData(SJTable, c.order, k, float32(v.Value))
								}
								if v.Value == 1 {
									c.deviceStatus[2] = masterWarning
								}
								if PrintDebug == 4 || PrintDebug == 9 {
									log.Println("SJTable", c.order, k, float32(v.Value))
								}
							}
						}
						c.deviceStatus[3] = cancleWarning
						for k := range wsysinfo.Yanwu {
							v := wsysinfo.Yanwu[k]

							if v.IsConnect == 0 && v.IsShield == 0 {
								c.deviceStatus[3] = masterWarning
							}

							if v.IsConnect == 1 && v.IsShield == 0 {
								if ok := c.checkSendDict(
									msgid, "yanwu"+strconv.Itoa(k), float32(v.Value)); ok {
									c.sendStorageData(YWTable, c.order, k, float32(v.Value))
								}
								if v.Value == 1 {
									c.deviceStatus[3] = masterWarning
								}
								if PrintDebug == 4 || PrintDebug == 9 {
									log.Println("YWTable", c.order, k, float32(v.Value))
								}
							}
						}


					}else if msgid == upsInfo && msgid / 10000 == c.clientType {
						//UPS信息
						var ups UpsInfo
						err := json.Unmarshal(message, &ups)
						if err != nil {
							c.log.Printf("upsInfo error: %v", err)
							break
						}
						c.deviceStatus[4] = cancleWarning

						for k := range ups.Ups {
							device := ups.Ups[k].Device
							v := ups.Ups[k]

							if v.IsConnect == 0 && v.IsShield == 0 {
								c.deviceStatus[4] = masterWarning
							}

							if v.IsConnect == 1 && v.IsShield == 0 {
								for n := range v.UpsValue {
									fvalue, err := strconv.ParseFloat(v.UpsValue[n], 32)
									if err != nil {
										//c.log.Printf("upsValue error: %v", err)
										//errCode = 1012
										//如果空值，忽略数据，不验证浮动数
										if PrintDebug == 4 || PrintDebug == 9 {
											log.Println("UPSTable", c.order, device, n)
										}
									} else if ok := c.checkSendDict(
										msgid, strconv.Itoa(device)+n, float32(fvalue)); ok {
										c.sendStorageDevice(UPSTable, c.order, device, n, v.UpsValue[n])
										if PrintDebug == 4 || PrintDebug == 9 {
											log.Println("UPSTable", c.order, device, n, v.UpsValue[n])
										}
									}

								}
								if v.IsAlarm == 1 {
									c.deviceStatus[4] = masterWarning
								}
							}
						}

					}else if msgid == airInfo && msgid / 10000 == c.clientType {
						//空调信息
						var air AirInfo
						err := json.Unmarshal(message, &air)
						if err != nil {
							c.log.Printf("airInfo error: %v", err)
							break
						}

						c.deviceStatus[5] = cancleWarning
						for k := range air.Air {
							device := air.Air[k].Device
							v := air.Air[k]

							if v.IsConnect == 0 && v.IsShield == 0 {
								c.deviceStatus[5] = masterWarning
							}

							if v.IsConnect == 1 && v.IsShield == 0 {
								for n := range v.AirValue {
									fvalue, err := strconv.ParseFloat(v.AirValue[n], 32)
									if err != nil {
										//c.log.Printf("airValue error: %v", err)
										//errCode = 1012
										//如果空值，忽略数据,不验证浮动数
										if PrintDebug == 4 || PrintDebug == 9 {
											log.Println("AirTable", c.order, device, n)
										}
									} else if ok := c.checkSendDict(
										msgid, strconv.Itoa(device)+n, float32(fvalue)); ok {
										c.sendStorageDevice(AIRTable, c.order, device, n, v.AirValue[n])
										if PrintDebug == 4 || PrintDebug == 9 {
											log.Println("AirTable", c.order, device, n, v.AirValue[n])
										}
									}
								}
								if v.IsAlarm == 1 {
									c.deviceStatus[5] = masterWarning
								}
							}
						}

					} else {
						errCode = 1110
						c.log.Println(strconv.Itoa(msgid) +"  MsgID not in list")
					}

					if errCode == 0 {
						c.exchange <- message
						//c.log.Println("c.exchange <- "+string(message))
					}

					//判断是否有主机设备告警
					isAlarm := cancleWarning
					for index := range c.deviceStatus {
						if c.deviceStatus[index] == masterWarning {
							isAlarm = masterWarning
						}
					}
					if c.sendStatus != isAlarm {
						var orders []string
						orders = append(orders,c.order)
						result := &DoCommand{
							isAlarm,
							c.order,
							orders,
						}
						b, _ := json.Marshal(result)
						c.hub.broadcast <- b
						c.sendStatus = isAlarm
						if PrintDebug == 3 ||  PrintDebug == 9 {
							log.Println("IsAlarm: ",c.order,"Warning999 Cancle333: ",isAlarm)
						}
					}

					c.send <- []byte("{\"MsgID\":\"" + strconv.Itoa(msgid+1) +
						"\",\"Err\":" + strconv.Itoa(errCode) + "}")

				}
			}
		}

	}
}

// writePump pumps messages from the hub to the websocket connection.
//
// A goroutine running writePump is started for each connection. The
// application ensures that there is at most one writer to a connection by
// executing all writes from this goroutine.
func (c *Client) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.conn.Close()
		c.log.Printf("writePump defer, close client order: %v",c.order)
	}()
	for {
		select {
		case client := <-c.register:
			c.browsers[client] = true
			c.writeSendData(onlineMaster)

		case client := <-c.unregister:
			if _, ok := c.browsers[client]; ok {
				delete(c.browsers, client)
				if len(c.browsers) == 0 {
					c.writeSendData(offlineMaster)
				}
				c.log.Println("client := <-c.unregister delete(c.browsers, client)")
			}
		case byteContent, ok := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				// The hub closed the channel.
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				c.log.Printf("byteContent, ok := <-c.send: %v",c.order)
				return
			}

			w, err := c.conn.NextWriter(websocket.TextMessage)
			if err != nil {
				c.log.Printf("byteContent, ok := <-c.send NextWriter: %v",err)
				return
			}

			w.Write(byteContent)

			// Add queued chat messages to the current websocket message.
			n := len(c.send)
			for i := 0; i < n; i++ {
				w.Write(newline)
				w.Write(<-c.send)
			}

			if err := w.Close(); err != nil {
				c.log.Printf("byteContent, ok := <-c.send close: %v",err)
				return
			}

		case byteContent, ok := <-c.exchange:
			if !ok {
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				c.log.Printf("byteContent, ok := <-c.exchange: %v",c.order)
				return
			}
			for client := range c.browsers {
				if _, ok := c.hub.clients[client]; ok {
					//c.log.Println("range c.browsers "+string(byteContent))
					select {
					case client.send <- byteContent:
						//c.log.Println("exchange to master: "+string(byteContent))
					default:
						//browser终端缓存已满，不再推送数据
						delete(c.browsers, client)
					}
				}else { //HUB delete Browser终端
					delete(c.browsers, client)
					if len(c.browsers) == 0 {
						c.writeSendData(offlineMaster)
					}
					c.log.Println("client := range c.browsers delete(c.browsers, client)")
				}
			}

		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, []byte{}); err != nil {
				c.log.Printf("<-ticker.C: %v",c.order)
				return
			}
		}
	}
}

func (c* Client)writeSendData(msgid int) {
	//result response
	c.conn.SetWriteDeadline(time.Now().Add(writeWait))
	w, err := c.conn.NextWriter(websocket.TextMessage)
	if err != nil {
		c.log.Printf("func (c* Client)writeSendData:NextWriter %v",err)
		return
	}
	byteContent := []byte("{\"MsgID\":\"" + strconv.Itoa(msgid) +
		"\",\"Err\":0}")

	w.Write(byteContent)
	if err := w.Close(); err != nil {
		c.log.Printf("func (c* Client)writeSendData:close %v",err)
		return
	}
}

func (c* Client)sendStorageData(table int, order string, key int, value float32)  {
	result := &DataInfo{
		table,
		order,
		strconv.Itoa(key),
		value,
	}
	b, _ := json.Marshal(result)
	c.storage.dataMsg <- b
	//println(string(b))
}

func (c* Client)sendStorageDevice(table int, order string, device int, key string, value string)  {
	result := &DeviceInfo{
		table,
		order,
		device,
		key,
		value,
	}
	b, _ := json.Marshal(result)
	c.storage.deviceMsg <- b
	//println(string(b))
}

func (c* Client)sendStorageMaster(order string, mac string, status int)  {
	result := &MasterInfo{
		order,
		mac,
		status,
	}
	b, _ := json.Marshal(result)
	c.storage.masterMsg <- b
	//println(string(b))
}

func (c* Client)sendStorageWarn(order string, name string, addr int, value string, error string, datetime string)  {
	result := &WarningInfo{
		order,
		name,
		addr,
		value,
		error,
		datetime,
	}
	b, _ := json.Marshal(result)
	c.storage.warningMsg <- b
}

func (c* Client)checkSendTime(key string) bool {
	times, ok := c.timeDict[key]
	if ok {
		if time.Now().Unix() - times > 3000 {
			c.timeDict[key] = time.Now().Unix()
			return true
		}else {
			return  false
		}

	}else {
		c.timeDict[key] = time.Now().Unix()
		return  true
	}
}

func (c *Client)checkSendDict(msgid int, addr string ,value float32) bool {
	key := strconv.Itoa(msgid) + addr
	content, ok := c.msgDict[key]
	if ok {
		if int(value) == 0{
			if int(content) == 1 {
				c.timeDict[key] = time.Now().Unix()
				c.msgDict[key] = value
				return true
			}else {
				return c.checkSendTime(key)
			}
		}else {
			if int((value-content)*10)/int(value) == 0 {
				return c.checkSendTime(key)
			}else {
				c.timeDict[key] = time.Now().Unix()
				c.msgDict[key] = value
				return true
			}
		}

	}else {
		c.msgDict[key] = value
		c.timeDict[key] = time.Now().Unix()
		return true
	}
}


// serveWs handles webSocket requests from the peer.
func serveWs(hub *Hub, storage *Storage, w http.ResponseWriter, r *http.Request,mlog *log.Logger) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		mlog.Println(err)
		return
	}

	client := newClient(hub,storage,conn,mlog)
	go client.writePump()
	client.readPump()
}
