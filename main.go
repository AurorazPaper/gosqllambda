package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"

	"github.com/aws/aws-lambda-go/lambda"
	_ "github.com/go-sql-driver/mysql"
)

var db *sql.DB

type faxLog struct {
	datetime string // date from xferfaxlog MM/dd/yy HH:mm, 24 HR clock
	// entrytype string //SEND,RECV,CALL,POLL,PAGE,UNSENT,SUBMIT,PROXY
	//commid      string
	//modem       string
	qfile string // SEND: jobid
	// jobtag      string // RECV: NULL
	// sender      string // The sender/receiver electronic mailing address (facsimile receptions are always attributed to the "fax" user).
	localnumber string // SEND: destnumber
	tsi         string // SEND: csi
	// params      string
	// npages int
	// jobtime     string
	// conntime    string
	// reason      string
	cidname   string // SEND: faxname
	cidnumber string // SEND: faxnumber
	// callid      string // SEND: empty
	// owner       string
	// dcs         string
	jobinfo string // totpages/ntries/ndials/totdials/maxdials/tottries/maxtries
	// system      string // zPaper: record source host name (part of passed in params)
	// did         string // zPaper: callid stripped of non-digits prefixed with leading 1 if necessary
}

type GoTestEvent struct {
	Name string `json:"name"`
}

func HandleRequestTest(ctx context.Context, event GoTestEvent) (string, error) {

	// Retrieve environment variables
	rdsHost := os.Getenv("RDS_HOST")
	rdsPort := os.Getenv("RDS_PORT")
	dbName := os.Getenv("DB_NAME")
	username := os.Getenv("DB_USER")
	password := os.Getenv("DB_PASSWORD")

	// Create connection string
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", username, password, rdsHost, rdsPort, dbName)

	// Connect to RDS
	var err error
	db, err = sql.Open("mysql", dsn)
	if err != nil {
		return "", fmt.Errorf("error connecting to database: %v", err)
	}
	defer db.Close()

	// Test the connection
	err = db.Ping()
	if err != nil {
		return "", fmt.Errorf("error pinging database: %v", err)
	}

	faxlogs, err := queryfaxrecords("RECV")
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("logs found: %v", faxlogs)

	return "Successfully connected to RDS", nil
}

func main() {

	lambda.Start(HandleRequestTest)

}

func queryfaxrecords(recordtype string) (interface{}, error) {
	var faxlogs []faxLog

	rows, err := db.Query("SELECT datetime, qfile, localnumber, tsi, cidname, cidnumber, jobinfo FROM xferfaxlog WHERE entrytype = ? LIMIT 25", recordtype)
	if err != nil {
		return nil, fmt.Errorf("queryfaxrecords %q: %v", recordtype, err)
	}
	defer rows.Close()
	// Loop through rows, using Scan to assign data from xferfaxlogs to struct fields
	for rows.Next() {
		var log faxLog

		log.datetime = ""
		log.qfile = ""
		log.localnumber = ""
		log.tsi = ""
		log.cidname = ""
		log.cidnumber = ""
		log.jobinfo = ""

		if err := rows.Scan(&log.datetime, &log.qfile, &log.localnumber, &log.tsi, &log.cidname, &log.cidnumber, &log.jobinfo); err != nil {
			return nil, fmt.Errorf("queryfaxrecords %q: %v", recordtype, err)
		}
		faxlogs = append(faxlogs, log)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("queryfaxrecords %q: %v", recordtype, err)
	}
	return faxlogs, nil

}
