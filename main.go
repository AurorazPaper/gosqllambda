package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	_ "github.com/go-sql-driver/mysql"
)

var db *sql.DB

type faxLog struct {
	datetime  string // date from xferfaxlog MM/dd/yy HH:mm, 24 HR clock
	entrytype string //SEND,RECV,CALL,POLL,PAGE,UNSENT,SUBMIT,PROXY
	commid    string
	//modem       string
	qfile string // SEND: jobid
	// jobtag      string // RECV: NULL
	// sender      string // The sender/receiver electronic mailing address (facsimile receptions are always attributed to the "fax" user).
	localnumber string // SEND: destnumber
	tsi         string // SEND: csi
	// params      string
	npages string
	// jobtime     string
	conntime string
	// reason      string
	cidname   string // SEND: faxname
	cidnumber string // SEND: faxnumber
	// callid      string // SEND: empty
	// owner       string
	// dcs         string
	jobinfo string // totpages/ntries/ndials/totdials/maxdials/tottries/maxtries
	system  string // zPaper: record source host name (part of passed in params)
	// did         string // zPaper: callid stripped of non-digits prefixed with leading 1 if necessary
}

type GoTestEvent struct {
	Name string `json:"name"`
}

func HandleRequestTest(ctx context.Context, event GoTestEvent) (string, error) {

	// Retrieve environment variables from Lambda config
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
		return "1", fmt.Errorf("error connecting to database: %v", err)
	}
	defer db.Close()

	// Test the connection
	err = db.Ping()
	if err != nil {
		return "2", fmt.Errorf("error pinging database: %v", err)
	}

	/*
		faxlogs, err := queryfaxrecords("RECV")
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("logs found: %v", faxlogs)
	*/

	var wg sync.WaitGroup

	wg.Add(3)

	go func() {
		defer wg.Done()
		if _, err := processMissedcalls(db); err != nil {
			log.Println("Failure to process missed calls:", err)
		}
	}()
	go func() {
		defer wg.Done()
		if _, err := processIncompleteFax(db); err != nil {
			log.Println("Failure to process incomplete faxes:", err)
		}
	}()
	/*
		go func() {
			defer wg.Done()
			if _, err := processNextSuccess(db); err != nil {
				log.Println("Failure to process next success logic:", err)
			}
		}()
	*/
	go func() {
		defer wg.Done()
		if _, err := missedCallDiff(db); err != nil {
			log.Println("Failure to process call difference logic:", err)
		}
	}()
	

	wg.Wait()

	defer db.Close()
	return "Successfully connected to RDS", nil

}

func main() {

	lambda.Start(HandleRequestTest)

}

// Processes Call entries, categorizes them as missed or not missed
func processMissedcalls(db *sql.DB) (string, error) {
	updateCallMissed :=
		`UPDATE xferfaxlog
		SET callMissed = CASE
			WHEN entrytype = 'CALL' AND (reason IS NOT NULL AND reason != '') THEN 1
			WHEN entrytype = 'CALL' AND (reason IS NULL OR reason = '') THEN 0
			ELSE callMissed
		END
		WHERE id IN (
			SELECT id
			FROM (
				SELECT id
				FROM xferfaxlog
				WHERE entrytype = 'CALL' AND callMissed IS NULL
				ORDER BY datetime DESC  
			) AS subquery
		);
		`

	_, err := db.Exec(updateCallMissed)
	if err != nil {
		return "", fmt.Errorf("could not run Missed Call logic: %w", err)
	}

	return "callMissed column creation successful", nil

}

func processNextSuccess(db *sql.DB) (string, error) {
	// Gets the ID of the next successful fax between two phone numbers
	updateNextSuccess := `
		UPDATE xferfaxlog AS missed
		SET nextSuccess = (
			SELECT next.id
			FROM (
				SELECT id, localnumber, cidname, datetime
				FROM xferfaxlog
				WHERE callMissed = 0
			) AS next
			WHERE next.localnumber = missed.localnumber
			AND next.cidname = missed.cidname
			AND next.datetime > missed.datetime
			ORDER BY next.datetime ASC
			LIMIT 1
		)
		WHERE missed.callMissed = 1 AND nextSuccess IS NULL
		LIMIT 250;
	`

	startTime := time.Now()
	if _, err := db.Exec(updateNextSuccess); err != nil {
		return "", fmt.Errorf("could not run next success logic: %w", err)
	}
	duration := time.Since(startTime)
	log.Printf("Next success logic executed in %s", duration)

	return "Next success processing successful", nil
}

// Processes Call entries, categorizes them as missed or not missed
func processIncompleteFax(db *sql.DB) (string, error) {
	updateCallMissed :=
		`UPDATE xferfaxlog
		SET faxincomplete = CASE
			WHEN entrytype = 'RECV' AND (reason IS NOT NULL AND reason != '') THEN 1
			WHEN entrytype = 'RECV' AND (reason IS NULL OR reason = '') THEN 0
			ELSE callMissed
		END
		WHERE id IN (
			SELECT id
			FROM (
				SELECT id
				FROM xferfaxlog
				WHERE entrytype = 'RECV' AND faxincomplete IS NULL
				ORDER BY datetime DESC  
			) AS subquery
		);
		`

	_, err := db.Exec(updateCallMissed)
	if err != nil {
		return "", fmt.Errorf("could not run incomplete fax logic: %w", err)
	}

	return "incomplete fax processing successful", nil

}

func missedCallDiff(db *sql.DB) (string, error) {
	// Gets the ID of the next successful fax between two phone numbers
	getMissDiff := `
		UPDATE xferfaxlog AS missed
		SET retrytime = (
			SELECT TIMESTAMPDIFF(MINUTE, missed.datetime, next.datetime)
			FROM (
				SELECT id, localnumber, cidname, datetime
				FROM xferfaxlog
				WHERE callMissed = 0
			) AS next
			WHERE next.localnumber = missed.localnumber
			AND next.cidname = missed.cidname
			AND next.datetime > missed.datetime
			ORDER BY next.datetime ASC
			LIMIT 1
		)
		WHERE missed.callMissed = 1 AND retrytime IS NULL
		LIMIT 250;
		`
	startTime := time.Now()
	if _, err := db.Exec(getMissDiff); err != nil {
		return "", fmt.Errorf("could not run next missed call difference logic: %w", err)
	}
	duration := time.Since(startTime)
	log.Printf("call difference logic executed in %s", duration)

	return "call difference processing successful", nil
}
