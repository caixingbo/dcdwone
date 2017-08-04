// Copyright 2017 The dcdwone Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"encoding/json"
	"strconv"
	"log"
)

const (
	masterLogin = 10050
	browserLogin = 20080

	wsysInfo = 10110
	upsInfo = 10210
	airInfo = 10310
	warnInfo = 11010

	onlineMaster = 12010
	offlineMaster = 12020
	openMasterDoor = 13010

	activeMaster = 19010

	takeOrder = 20110
	cancleOrder = 20120
	checkOnline = 20310

	openSuo = 23010

	masterWarning = 999
	cancleWarning = 333

	MasterType = 1
	BrowserType = 2

)

//内部交换的数据封装

type DoCommand struct {
	Command int
	MyID string
	Orders []string
}

type OnlineMaster struct {
	MsgID string
	ErrCode int
	Masters []Master
}

type Master struct {
	Order string
	IsAlarm int
}

//服务接收的数据报文

type MasterLogin struct {
	MsgID string
	Order string
	Token string
	Mac   string
}

type BrowserLogin struct {
	MsgID string
	Name string
	Token string
}

type TakeOrder struct {
	MsgID string
	Name string
	Orders []string
}

type DeleteOrder struct {
	MsgID string
	Name string
	Orders []string
}


type WsysInfo struct {
	MsgID string
	Order string
	WSdu []TempHD
	Shuijin []WaterSM
	Yanwu []WaterSM
}

type TempHD struct {
	Addr int
	Tvalue float32
	Hvalue float32
	TAlarm int
	HAlarm int
	IsConnect int
	IsShield int
}

type WaterSM struct {
	Addr int
	Value int
	IsConnect int
	IsShield int
}

type AirInfo struct {
	MsgID string
	Order string
	Air []AirDevice
}

type AirDevice struct {
	Device int
	AirValue map[string]string
	IsAlarm int
	IsConnect int
	IsShield int
	AlarmInfo string
}

type UpsInfo struct {
	MsgID string
	Order string
	Ups []UpsDevice
}

type UpsDevice struct {
	Device int
	UpsValue map[string]string
	IsAlarm int
	IsConnect int
	IsShield int
	AlarmInfo string
}

type Warning struct {
	MsgID string
	Order string
	Name  string
	Address int
	Value string
	DateTime string
	WarnInfo string
}

type OpenSuo struct {
	MsgID string
	Name string
	Orders []string
}

type CheckOnline struct {
	MsgID string
	Name string
}

// hub maintains the set of active clients and broadcasts messages to the
// clients.
type Hub struct {
	// Order map
	orders map[string]*Client

	// Order status
	status map[string]int

	// Registered clients.
	clients map[*Client]bool

	// Inbound messages from the clients.
	broadcast chan []byte

	// Register requests from the clients.
	register chan *Client

	// Unregister requests from clients.
	unregister chan *Client

	storage *Storage

	log *log.Logger
}

func newHub(storage *Storage, log *log.Logger) *Hub {
	return &Hub{
		orders:     make(map[string]*Client),
		broadcast:  make(chan []byte),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		clients:    make(map[*Client]bool),
		status:     make(map[string]int),
		storage:storage,
		log : log,

	}
}

func (h *Hub) sendMasterInfo() {
	var masters []Master
	for zid := range  h.storage.zidmap {
		order := strconv.Itoa(zid)
		link := 2
		if online ,ok := h.status[order]; ok{
			link = online
		}
		m := &Master{
			Order:order,
			IsAlarm:link,
		}
		masters = append(masters,*m)
	}

	result := &OnlineMaster{
		strconv.Itoa(checkOnline+1),
		0,
		masters,
	}
	b, _ := json.Marshal(result)

	for index := range h.orders {
		if len(index) > 6 {
			h.orders[index].send <-b
		}
	}
}

func (h *Hub) run() {
	for {
		select {
		case client := <-h.register:
			h.clients[client] = true
			h.orders[client.order] = client
			h.status[client.order] = 0
			//如果主机登陆连接，发送广播告警通知
			if len(client.order) < 7 {
				h.sendMasterInfo()
			}
		case client := <-h.unregister:
			if _, ok := h.clients[client]; ok {
				delete(h.clients,client)
				delete(h.orders,client.order)
				delete(h.status,client.order)
				close(client.send)
				close(client.exchange)
				//如果主机失去连接，发送广播告警通知
				if len(client.order) < 7 {
					h.sendMasterInfo()
				}
			}
		case message := <-h.broadcast:
			h.log.Println(string(message))
			var do DoCommand
			json.Unmarshal(message, &do)
			browser := h.orders[do.MyID]
			command := do.Command
			if command == takeOrder {
				for index := range do.Orders {
					if master ,ok := h.orders[do.Orders[index]]; ok{
						master.register <- browser
					}
				}
			} else if command == cancleOrder {
				for index := range do.Orders {
					if master ,ok := h.orders[do.Orders[index]]; ok {
						master.unregister <- browser
					}
				}
			} else if command == masterWarning {
				for index := range do.Orders {
					h.status[do.Orders[index]] = 1
				}
				h.sendMasterInfo()
			} else if command == cancleWarning {
				for index := range do.Orders {
					h.status[do.Orders[index]] = 0
				}
				h.sendMasterInfo()
			}else if command == openSuo {
				for index := range do.Orders {
					if master ,ok := h.orders[do.Orders[index]]; ok {
						master.send <- []byte("{\"MsgID\":\"" + strconv.Itoa(openMasterDoor) +
							"\",\"Err\":0}")
					}
				}
			}
		}
	}
}

