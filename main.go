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

type goLogicEvent struct {
	Name string `json:"name"`
}

func handleRequest(ctx context.Context, event goLogicEvent) (string, error) {

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

	if err := createMissedFaxLogTable(db); err != nil {
		fmt.Printf("Error creating table: %s\n", err)
	} else {
		fmt.Println("missed_faxlog table created or already exists.")
	}

	// sync is go's version of async - handles concurrency
	var wg sync.WaitGroup

	// adds 2 functions to the waitgroup,
	wg.Add(2)
	// sorts all CALL entries into missed or not missed
	go func() {
		defer wg.Done()
		if _, err := processMissedcalls(db); err != nil {
			log.Println("Failure to process missed calls:", err)
		}
	}()
	// grabs difference between missed call and the next successful call
	// writes to retrytime column
	go func() {
		defer wg.Done()
		if _, err := missedCallDiff(db); err != nil {
			log.Println("Failure to process call difference logic:", err)
		}
	}()

	wg.Wait()

	defer db.Close()
	return "Go functions run successfully", nil

}

func main() {

	lambda.Start(handleRequest)

}

// Processes Call entries, categorizes them as missed or not missed
func processMissedcalls(db *sql.DB) (string, error) {
	updateCallMissed :=
		`UPDATE theBigTable
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

func missedCallDiff(db *sql.DB) (string, error) {
	// Gets the ID of the next successful fax between two phone numbers
	getMissDiff := `
		UPDATE xferfaxlog base
		INNER JOIN (
			SELECT missed.id, 
				TIMESTAMPDIFF(MINUTE, missed.datetime, MIN(next.datetime)) as retry_mins
			FROM xferfaxlog missed
			INNER JOIN xferfaxlog next ON 
				next.localnumber = missed.localnumber AND 
				next.cidname = missed.cidname AND
				next.datetime > missed.datetime AND
				next.callMissed = 0
			WHERE missed.callMissed = 1
			AND missed.retrytime IS NULL
			GROUP BY missed.id, missed.datetime
			LIMIT 10000
		) AS next_calls ON base.id = next_calls.id
		SET base.retrytime = LEAST(next_calls.retry_mins, 360)
		WHERE base.retrytime IS NULL;
		`
	startTime := time.Now()
	if _, err := db.Exec(getMissDiff); err != nil {
		return "", fmt.Errorf("could not run next missed call difference logic: %w", err)
	}
	duration := time.Since(startTime)
	log.Printf("call difference logic executed in %s", duration)

	return "call difference processing successful", nil
}

func createMissedFaxLogTable(db *sql.DB) error {
	createTableSQL := `
    CREATE TABLE IF NOT EXISTS missed_faxlog (
        id INT AUTO_INCREMENT PRIMARY KEY,
        xferfaxlog_id INT NOT NULL,
        localnumber VARCHAR(255),
        cidname VARCHAR(255),
        datetime DATETIME,
        retrytime INT,
        FOREIGN KEY (xferfaxlog_id) REFERENCES xferfaxlog(id) ON DELETE CASCADE,
        INDEX (localnumber),
        INDEX (cidname),
        INDEX (datetime)
    );`

	// Execute the create table statement
	_, err := db.Exec(createTableSQL)
	if err != nil {
		return fmt.Errorf("could not create missed_faxlog table: %w", err)
	}

	return nil
}
