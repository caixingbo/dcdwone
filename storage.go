package main

import (
	"database/sql"

	_ "github.com/go-sql-driver/mysql"

	"time"
	"encoding/json"
	"strconv"
	"log"
)

const (
	WDTable = iota
	SDTable
	SJTable
	YWTable
	UPSTable
	AIRTable
)

const  (
	maxInt64Recode = 1000000000000000000 //Int64不越界
	waitTimeCheck = 3600*24    //每天检查一次
	saveTimeCheck = 3600*24*180 //数据保留180天
)


type DataInfo struct {
	Table int
	Order string
	Key   string
	Value float32
}

type DeviceInfo struct {
	Table int
	Order string
	Device int
	Key   string
	Value string
}

type MasterInfo struct {
	Order string
	Mac   string
	Status int
}

type WarningInfo struct {
	Order string
	Name  string
	Addr  int
	Value string
	Error string
	DateTime string
}

type  Storage struct{
	zidmap map[int]int
	zidtmp map[int]int
	dataMsg chan []byte
	deviceMsg chan []byte
	masterMsg chan []byte
	warningMsg chan []byte
	db *sql.DB
	log *log.Logger
}

func newStorage(log *log.Logger) *Storage{
	return  &Storage{
		zidmap:     make(map[int]int),
		zidtmp:     make(map[int]int),
		dataMsg:    make(chan []byte,256),
		deviceMsg:  make(chan []byte,256),
		masterMsg:  make(chan []byte,256),
		warningMsg: make(chan []byte,256),
		log : log,
	}
}

func (s *Storage)checkErr(err error) {
	if err != nil {
		s.log.Printf("storage error: %v", err)
		panic(err)
	}
}

func (s *Storage) connectDB(dbStr string, connStr string){
	defer func() {
		if r := recover(); r != nil {
			s.log.Printf("db connect error caught: %v", r)
		}
	}()
	db, err := sql.Open(dbStr, connStr)
	s.checkErr(err)
	s.db = db

	//查询主机Order
	rows, err := s.db.Query("select zid from zhuji")
	s.checkErr(err)

	//初始化zidtmp
	for k := range s.zidtmp {
		s.zidtmp[k] = 0
	}

	//处理临时交换数据
	order:=0
	for rows.Next() {
		err = rows.Scan(&order)
		s.checkErr(err)
		s.zidtmp[order] = 1
	}

	for k := range s.zidmap {
		if  v ,ok := s.zidtmp[k]; ok {
			if v == 1 {
				s.zidtmp[k] = 2
			}else {
				s.zidtmp[k] = -1
			}
		} else {
			s.zidtmp[k] = -1
		}
	}

	//更新主机列表
	for k := range s.zidtmp {
		if s.zidtmp[k] == -1 {
			delete(s.zidmap,k)
		}
		if s.zidtmp[k] == 1 {
			s.zidmap[k] = 0
		}
	}

}

func (s *Storage) existQueryDB(sqlStr string) bool {
	rows, err := s.db.Query(sqlStr)
	s.checkErr(err)
	return rows.Next()
}

func (s *Storage) insertData(message []byte) {
	defer func() {
		if r := recover(); r != nil {
			s.log.Printf("insertdata error caught: %v", r)
		}
	}()
	var dataInfo DataInfo
	err := json.Unmarshal(message, &dataInfo)
	s.checkErr(err)

	var querySQL string
	if dataInfo.Table == WDTable {
		querySQL = "INSERT wendu (zid,no,value,datetime) values (?,?,?,?)"
	}else if dataInfo.Table == SDTable {
		querySQL = "INSERT shidu (zid,no,value,datetime) values (?,?,?,?)"
	}else if dataInfo.Table == SJTable {
		querySQL = "INSERT shuijin (zid,no,status,datetime) values (?,?,?,?)"
	}else if dataInfo.Table == YWTable {
		querySQL = "INSERT yanwu (zid,no,status,datetime) values (?,?,?,?)"
	}
	stmt, err := s.db.Prepare(querySQL)
	s.checkErr(err)
	datetime := time.Now().Format("2006-01-02 15:04:05")
	order,_ := strconv.Atoi(dataInfo.Order)

	if dataInfo.Table == SJTable || dataInfo.Table == YWTable {
		_, err := stmt.Exec(order, dataInfo.Key, int(dataInfo.Value), datetime)
		s.checkErr(err)
		//id, err := res.LastInsertId()
		//s.checkErr(err)
		//s.log.Println(id,order, dataInfo.Key, dataInfo.Value, datetime)
	}else {
		_, err := stmt.Exec(order, dataInfo.Key, dataInfo.Value, datetime)
		s.checkErr(err)
		//id, err := res.LastInsertId()
		//s.checkErr(err)
		//s.log.Println(id,order, dataInfo.Key, dataInfo.Value, datetime)
	}

}


func (s *Storage) insertDevice(message []byte) {
	defer func() {
		if r := recover(); r != nil {
			s.log.Printf("insertdevice error caught: %v", r)
		}
	}()
	var deviceInfo DeviceInfo
	err := json.Unmarshal(message, &deviceInfo)
	s.checkErr(err)

	var querySQL string

	if deviceInfo.Table == UPSTable {
		querySQL = "INSERT ups (zid,no,name,value,datetime) values (?,?,?,?,?)"
	}else if deviceInfo.Table == AIRTable {
		querySQL = "INSERT kongtiao (zid,no,name,value,datetime) values (?,?,?,?,?)"
	}

	stmt, err := s.db.Prepare(querySQL)
	s.checkErr(err)
	datetime := time.Now().Format("2006-01-02 15:04:05")
	order,_ := strconv.Atoi(deviceInfo.Order)
	_, err = stmt.Exec(order, deviceInfo.Device, deviceInfo.Key, deviceInfo.Value, datetime)
	s.checkErr(err)
	//id, err := res.LastInsertId()
	//s.checkErr(err)
	//s.log.Println(id,order, deviceInfo.Device, deviceInfo.Key, deviceInfo.Value, datetime)
}


func (s *Storage) insertWarning(message []byte) {
	defer func() {
		if r := recover(); r != nil {
			s.log.Printf("insert warning error caught: %v", r)
		}
	}()
	var warning WarningInfo
	err := json.Unmarshal(message, &warning)
	s.checkErr(err)

	querySQL := "INSERT warning (zid,no,name,error,value,datetime) values (?,?,?,?,?,?)"
	stmt, err := s.db.Prepare(querySQL)
	s.checkErr(err)
	datetime, _ := time.Parse("2006-01-02 15:04:05", warning.DateTime)
	order,_ := strconv.Atoi(warning.Order)
	res, err := stmt.Exec(order, warning.Addr, warning.Name, warning.Error,warning.Value, datetime)
	s.checkErr(err)
	id, err := res.LastInsertId()
	s.checkErr(err)
	if PrintDebug == 5 ||  PrintDebug == 9 {
		log.Println("db warn:",id,warning.Name,warning.Order,warning.Value,warning.Error,datetime.String())
	}

}

func (s *Storage) updateMaster(message []byte) {
	defer func() {
		if r := recover(); r != nil {
			s.log.Printf("update master error caught: %v", r)
		}
	}()
	var master MasterInfo
	err := json.Unmarshal(message, &master)
	s.checkErr(err)

	if ok := s.existQueryDB("select id from zhuji where zid = " + master.Order); ok{
		stmt, err := s.db.Prepare("UPDATE zhuji SET mac=? WHERE zid=?")
		s.checkErr(err)
		order,_ := strconv.Atoi(master.Order)
		res, err := stmt.Exec(master.Mac, order)
		s.checkErr(err)
		num, err := res.RowsAffected()
		s.checkErr(err)
		if PrintDebug == 6 ||  PrintDebug == 9 {
			log.Println("update zhuji", num, order, master.Mac, master.Status)
		}

	}else {
		querySQL := "INSERT zhuji (zid,mac,status) values (?,?,?)"
		stmt, err := s.db.Prepare(querySQL)
		s.checkErr(err)
		order,_ := strconv.Atoi(master.Order)
		res, err := stmt.Exec(order, master.Mac, master.Status)
		s.checkErr(err)
		id, err := res.LastInsertId()
		s.checkErr(err)
		if PrintDebug == 6 ||  PrintDebug == 9 {
			log.Println("insert zhuji", id, order, master.Mac, master.Status)
		}
	}

	//查询主机Order
	rows, err := s.db.Query("select zid from zhuji")
	s.checkErr(err)

	//初始化zidtmp
	for k := range s.zidtmp {
		s.zidtmp[k] = 0
	}

	//处理临时交换数据
	order:=0
	for rows.Next() {
		err = rows.Scan(&order)
		s.checkErr(err)
		s.zidtmp[order] = 1
	}

	for k := range s.zidmap {
		if  v ,ok := s.zidtmp[k]; ok {
			if v == 1 {
				s.zidtmp[k] = 2
			}else {
				s.zidtmp[k] = -1
			}
		} else {
			s.zidtmp[k] = -1
		}
	}

	//更新主机列表
	for k := range s.zidtmp {
		if s.zidtmp[k] == -1 {
			delete(s.zidmap,k)
		}
		if s.zidtmp[k] == 1 {
			s.zidmap[k] = 0
		}
	}
}

func (s *Storage) checkDataBase(tableID int) {
	defer func() {
		if r := recover(); r != nil {
			s.log.Printf("check Database error caught: %v", r)
		}
	}()

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
	rows, err := s.db.Query("select count(*) from "+ tableName)
	s.checkErr(err)
	count := 0
	for rows.Next() {
		err = rows.Scan(&count)
		s.checkErr(err)
	}

	//如果记录少于10000条不清理
	if count < 10000 {
		return
	}

	stmt, err := s.db.Prepare("DELETE FROM " + tableName + " WHERE datetime < ?")
	s.checkErr(err)
	timestamp := time.Now().Unix() - saveTimeCheck
	tm := time.Unix(timestamp, 0)
	datetime :=  tm.Format("2006-01-02 15:04:05")
	res, err := stmt.Exec(datetime)
	s.checkErr(err)
	num, err := res.RowsAffected()
	s.checkErr(err)
	s.log.Printf(tableName +" table Rows count %v ,RowsAffected %v",count,num)
	if PrintDebug == 7 ||  PrintDebug == 9 {
		log.Printf(tableName +" table Rows count %v ,RowsAffected %v datetime < %v",count,num,datetime)
	}
}

func (s *Storage) run()  {
	var data int64 = 0
	var device int64 = 0
	var times = time.Now().Unix()
	var tableID = WDTable
	for{
		select {
		case message := <-s.dataMsg:
			s.insertData(message)
			data++
			if data%100 == 0 {
				s.log.Printf("db insert data total:%v",data)
			}
			if data > maxInt64Recode {data = 0}
			if time.Now().Unix() - times > waitTimeCheck{
				//每隔一段时间，检查一张表数据，已减少等待时间
				times = time.Now().Unix()
				s.checkDataBase(tableID)
				tableID += 1
				if tableID > AIRTable {tableID = WDTable}
			}

		case message := <-s.deviceMsg:
			s.insertDevice(message)
			device++
			if device%100 == 0 {
				s.log.Printf("db insert device total:%v",device)
			}
			if device > maxInt64Recode {device = 0}

		case message := <-s.warningMsg:
			s.insertWarning(message)
		case message := <-s.masterMsg:
			s.updateMaster(message)
		}
	}

}








