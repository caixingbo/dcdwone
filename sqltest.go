package main

import (
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
	"time"
	"log"
)


/*
db_host = 121.43.165.4
db_port = 3306
db_user = dc_admin
db_pass = dc*2017
db_base = dc
*/
var (
	dbhostsip  = "127.0.0.1:3306"     //IP地址
	dbusername = "root"               //用户名
	dbpassword = "dtct123456"         //密码
	dbname     = "datacenter.warning" //表名
)

func main() {
	//db, err := sql.Open("mysql", "dc_admin:dc*2017@tcp(121.43.165.4:3306)/dc?charset=utf8")
	db, err := sql.Open("mysql", "root:dtct123456@tcp(127.0.0.1:3306)/datacenter?charset=utf8")
	checkDBErr(err)

	tableID := WDTable
	var tableName string
	switch tableID {
	case WDTable: tableName = "wendu"
	case SDTable: tableName = "shidu"
	case SJTable: tableName = "shuijin"
	case YWTable: tableName = "yanwu"
	case UPSTable: tableName = "ups"
	case AIRTable: tableName = "kongtiao"
	}

	//查询数据
	rows, err := db.Query("select count(*) from "+ tableName)
	checkDBErr(err)

	count := 0
	for rows.Next() {
		err = rows.Scan(&count)
		checkDBErr(err)
	}
	fmt.Println(count)

	stmt, err := db.Prepare("delete FROM " + tableName + " WHERE datetime < ?")
	checkDBErr(err)
	timestamp := time.Now().Unix() - 3600*24*3
	fmt.Println(timestamp)
	//格式化为字符串,tm为Time类型
	tm := time.Unix(timestamp, 0)
	datetime :=  tm.Format("2006-01-02 15:04:05")
	fmt.Println(datetime)
	res, err := stmt.Exec(datetime)
	checkDBErr(err)
	num, err := res.RowsAffected()
	checkDBErr(err)
	log.Printf("RowsAffected %v",num)
	db.Close()
}

func checkDBErr(err error) {
	if err != nil {
		panic(err)
	}
}
