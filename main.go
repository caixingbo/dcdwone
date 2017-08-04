// Copyright 2013 The Gorilla WebSocket Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"flag"
	"log"
	"net/http"
	"github.com/robfig/config"
	"os"
)

//var addr = flag.String("addr", ":8080", "http service address")
var PrintDebug = 0
// 0 not print
// 1 receive
// 3 alarm
// 4 chan storage
// 5 warn table
// 6 master table
// 7 delete data
// 8 active
// 9 print all

func serveHome(w http.ResponseWriter, r *http.Request) {
	//log.Println(r.URL)
	if r.URL.Path != "/" {
		http.Error(w, "Not found", 404)
		return
	}
	if r.Method != "GET" {
		http.Error(w, "Method not allowed", 405)
		return
	}
	http.ServeFile(w, r, "home.html")
}

func main() {
	//创建日志文件
	file, err := os.OpenFile("dw.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, os.ModePerm|os.ModeTemporary)
	if err != nil {
		log.Fatalln("fail to create dw.log file!")
	}
	logger := log.New(file, "", log.LstdFlags)

	//读取配置文件
	flag.Parse()

	host := "addr"
	port := "7058"

	driver := "mysql"
	user := "dc_admin"
	password := "dc*2017"
	dbport := "3306"
	dbhost := "127.0.0.1"
	database := "dc"

	c, err := config.ReadDefault("dw.cfg")
	if err != nil {
		logger.Println("file dw.cfg not exist，use default config")
	}else {
		h, err := c.String("server", "host")
		if err == nil {host = h}
		p, err := c.String("server", "port")
		if err == nil {port = p}

		dd, err := c.String("db", "driver")
		if err == nil {driver = dd}
		dh, err := c.String("db", "host")
		if err == nil {dbhost = dh}
		dp, err := c.String("db", "port")
		if err == nil {dbport = dp}
		du, err := c.String("db", "user")
		if err == nil {user = du}
		pw, err := c.String("db", "password")
		if err == nil {password = pw}
		db, err := c.String("db", "database")
		if err == nil {database = db}

		pt, err := c.Int("debug", "print")
		if err == nil {PrintDebug = pt}

	}

	dbStr := driver
	connStr := user+":"+password+"@tcp("+dbhost+":"+dbport+")/"+database+"?charset=utf8"
	addr := flag.String(host, ":"+port ,"http service address")

	//创建运行服务
	storage := newStorage(logger)
	storage.connectDB(dbStr, connStr)
	go storage.run()

	hub := newHub(storage,logger)
	go hub.run()

	http.HandleFunc("/", serveHome)
	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		serveWs(hub, storage, w, r, logger)
	})
	logger.Println("dw center server start......")

	err = http.ListenAndServe(*addr, nil)
	if err != nil {
		log.Fatal("ListenAndServe: ", err)

	}

}



