package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/jmoiron/sqlx"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

type Receive struct {
	Date time.Time `json:"date"`
	Message_id string `json:"message_id"`
	User_message_reference int64 `json:"user_message_reference"`
	Sub string `json:"sub"`
	Dlvrd string `json:"dlvrd"`
	Submit_date string `json:"submit_date"`
	Done_date string `json:"done_date"`
	Stat string `json:"stat"`
	Err string `json:"err"`
	Text string `json:"text"`
	Source_addr string `json:"source_addr"`
	Destination_addr string `json:"destination_addr"`
}



//CREATE TABLE IF NOT EXISTS Receive (
//date DateTime,
//message_id String,
//user_message_reference Int64,
//sub String,
//dlvrd String,
//submit_date String,
//done_date String,
//stat String,
//err String,
//text String,
//source_addr String,
//destination_addr String
//) engine=Memory


//CREATE TABLE IF NOT EXISTS Receive (
//date DateTime,
//message_id String,
//user_message_reference Int32,
//sub String,
//dlvrd String,
//submit_date String,
//done_date String,
//stat String,
//err String,
//text String,
//source_addr String,
//destination_addr String
//) ENGINE = MergeTree()
//ORDER BY tuple()
//SETTINGS index_granularity = 8192

type sentJson struct {
	Id int `json:"id"`
	Sms_id int `json:"sms_id"`
	Sms_text string `json:"sms_text,omitempty"`
	Source_addr string `json:"source_addr"`
	Dest_addr string `json:"dest_addr"`
}

type Sent struct {
	Date time.Time `json:"date"`
	SentJson sentJson `json:"sentJson"`
	Sequence int64 `json:"sequence"`
}


//CREATE TABLE IF NOT EXISTS Send (
//date DateTime,
//id Int32,
//sms_id Int32,
//sms_text String,
//source_addr String,
//dest_addr String,
//sequence Int64
//) engine= MergeTree()
//ORDER BY tuple()
//SETTINGS index_granularity = 8192


type SentMesId struct {
	Message_id string `json:"message_id"`
	Sequence int64 `json:"sequence"`
}


//CREATE TABLE IF NOT EXISTS SentMesId (
//message_id String,
//sequence Int64
//) engine=MergeTree()
//ORDER BY tuple()
//SETTINGS index_granularity = 8192

func main()  {


	db, err := sqlx.Open("clickhouse", "tcp://dockerhost:9000?debug=true")
	if err != nil {
		log.Println("err 1", err)
		log.Fatal(err)
	}

	path := "/res/"

	dir, err := ioutil.ReadDir(path)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("ok path")

	for i, files := range dir {
		log.Println(string(i), files.Name(), "start")
		if (files.IsDir() != true){
			log.Println(string(i), files.Name(), "wait")
			//receiveLog(path + files.Name(), db)
			sentLog(path + files.Name(), db, strconv.Itoa(i))
			log.Println(string(i), files.Name(), "ok")
		}
	}
	}

func sentLog(fileP string, db *sqlx.DB, ind string)  {

	file, err := os.Open(fileP)

	if err!= nil{
		log.Fatal(file)
	}

	defer file.Close()

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {

		//var rec Receive
		log.Println("log step:",ind, fileP)
		fmt.Println(scanner.Text())

		text := strings.ReplaceAll(scanner.Text(), "None", " ")

		s := strings.Split(text, " ")

		layout := "2006-01-02 15:04:05"

		date, err := time.Parse(layout,s[0] + " " + strings.Split(strings.ReplaceAll(s[1], "]", ""), ",")[0])

		if err != nil {
			log.Println( "parse", err)
		}

		log.Println("ok")
		log.Println(s[2])

		if (s[2] == "send" && strings.HasPrefix(s[3],"message") == true){

			log.Println("ok send message with insert")

			sIndexStartJson := strings.Index(text, "{")
			sIndexStopJson := strings.Index(text, "sequence:")

			sTemp := text[sIndexStartJson:sIndexStopJson]

			fmt.Println(sTemp)
			var sentJs sentJson

			err := json.Unmarshal([]byte(strings.ReplaceAll(sTemp, "'", "\"")), &sentJs)
			if err != nil {
				fmt.Println(err)
			}


			sIndexStartJson = strings.Index(text, "sequence:")
			sIndexStopJson = strings.Index(text, "\n")

			sTemp = text[sIndexStartJson:len(scanner.Text())]


			s := strings.Split(sTemp, " ")

			sen, err := strconv.ParseInt(strings.Split(s[0], ":")[1], 10,32)

			rec := Sent{date, sentJs, sen}


			tx := db.MustBegin()

			tx.MustExec("INSERT INTO Send (date,id,sms_id,sms_text, source_addr,dest_addr,sequence ) VALUES ($1, $2, $3, $4, $5, $6, $7)", rec.Date, rec.SentJson.Id, rec.SentJson.Sms_id, rec.SentJson.Sms_text, rec.SentJson.Source_addr, rec.SentJson.Dest_addr,  rec.Sequence)

			err1 := tx.Commit()

			if err1 != nil {
				log.Println("log err1", err1)
				db, err = sqlx.Open("clickhouse", "tcp://dockerhost:9000?debug=true")
				if err != nil {
					log.Fatal(err)
				}
				tx.MustExec("INSERT INTO Send (date,id,sms_id,sms_text, source_addr,dest_addr,sequence ) VALUES ($1, $2, $3, $4, $5, $6, $7)", rec.Date, rec.SentJson.Id, rec.SentJson.Sms_id, rec.SentJson.Sms_text, rec.SentJson.Source_addr, rec.SentJson.Dest_addr, rec.Sequence)
				tx.Commit()
			}
		}

		if (strings.HasPrefix(s[2],"message_id") == true) {

			log.Println("ok message_id with insert")

			sen, err := strconv.ParseInt(strings.Split(s[3], ":")[1], 10,32)

			rec := SentMesId{strings.Split(s[2], ":")[1], sen}

			tx := db.MustBegin()

			tx.MustExec("INSERT INTO SentMesId (message_id, sequence ) VALUES ($1, $2)", rec.Message_id, rec.Sequence)

			err1 := tx.Commit()

			if err1 != nil {
				log.Println("log err1", err1)
				db, err = sqlx.Open("clickhouse", "tcp://dockerhost:9000?debug=true")
				if err != nil {
					log.Fatal(err)
				}
				tx.MustExec("INSERT INTO SentMesId (message_id, sequence ) VALUES ($1, $2)", rec.Message_id, rec.Sequence)

				tx.Commit()
			}
		}

	}
}

func receiveLog(fileP string, db *sqlx.DB)  {
	file, err := os.Open(fileP)

	if err!= nil{
		fmt.Println("err")
		log.Fatal(err)
	}

	defer file.Close()

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {

		//var rec Receive
		fmt.Println(scanner.Text())
		s := strings.Split(scanner.Text(), " ")
		layout := "2006-01-02 15:04:05"

		date, err := time.Parse(layout,s[0] + " " + strings.Split(strings.ReplaceAll(s[1], "]", ""), ",")[0])


		if err != nil {
			log.Panic(err)
		}
		user_mes_re, err := strconv.ParseInt(strings.Split(s[3], ":")[1], 10, 32)

		rec := Receive{date,
			strings.Split(s[2], ":")[1],
			user_mes_re,
			strings.Split(s[4], ":")[1],
			strings.Split(s[5], ":")[1],
			strings.Split(s[6], ":")[1],
			strings.Split(s[7], ":")[1],
			strings.Split(s[8], ":")[1],
			strings.Split(s[9], ":")[1],
			strings.Split(s[10], ":")[1],
			strings.Split(s[11], ":")[1],
			strings.Split(s[12], ":")[1]}

		tx := db.MustBegin()

		tx.MustExec("INSERT INTO Receive (date, message_id, user_message_reference, sub, dlvrd, submit_date, done_date, stat, err, text,source_addr,destination_addr) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)", rec.Date,rec.Message_id, rec.User_message_reference,rec.Sub,rec.Dlvrd, rec.Submit_date, rec.Done_date, rec.Stat, rec.Err, rec.Text, rec.Source_addr, rec.Destination_addr)

		err1 := tx.Commit()

		if err1 != nil {
			log.Println("log err1", err1)
			db, err = sqlx.Open("clickhouse", "tcp://dockerhost:9000?debug=true")
			if err != nil {
				log.Fatal(err)
			}
			tx.MustExec("INSERT INTO Receive (date, message_id, user_message_reference, sub, dlvrd, submit_date, done_date, stat, err, text,source_addr,destination_addr) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)", rec.Date,rec.Message_id, rec.User_message_reference,rec.Sub,rec.Dlvrd, rec.Submit_date, rec.Done_date, rec.Stat, rec.Err, rec.Text, rec.Source_addr, rec.Destination_addr)
			tx.Commit()
		}
	}
}
