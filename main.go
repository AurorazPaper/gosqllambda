package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/aws/aws-lambda-go/lambda"
	_ "github.com/go-sql-driver/mysql"
)

var db *sql.DB

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

	if err := createMissedFaxLogTable(db); err != nil {
		fmt.Printf("Error creating table: %s\n", err)
	} else {
		fmt.Println("missed_faxlog table created or already exists.")
	}

	var wg sync.WaitGroup

	wg.Add(2)

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
		SET callMissed = 
			CASE
				WHEN entrytype = 'CALL' AND (reason IS NOT NULL AND reason != '') THEN 1
				WHEN entrytype = 'CALL' AND (reason IS NULL OR reason = '') THEN 0
				ELSE callMissed
			END
		WHERE id IN (
			SELECT id FROM (
				SELECT id
				FROM xferfaxlog
				WHERE entrytype = 'CALL' AND callMissed IS NULL
				ORDER BY datetime DESC
				LIMIT 5000
			) AS subquery
		);
		`

	_, err := db.Exec(updateCallMissed)
	if err != nil {
		return "", fmt.Errorf("could not run Missed Call logic: %w", err)
	}

	return "callMissed column creation successful", nil

}

// Processes Call entries, categorizes them as missed or not missed
func processIncompleteFax(db *sql.DB) (string, error) {
	updateCallMissed :=
		`UPDATE xferfaxlog AS missed
		SET faxincomplete = incomplete
		WHERE npages != 0
  			AND column2 IS NOT NULL;
		`

	_, err := db.Exec(updateCallMissed)
	if err != nil {
		return "", fmt.Errorf("could not run incomplete fax logic: %w", err)
	}

	return "incomplete fax processing successful", nil

}
func missedCallDiff(db *sql.DB) (string, error) {
	insertMissedCalls := `
        INSERT INTO missed_faxlog (id, localnumber, cidname, retrytime)
		SELECT
			missed.id,
			missed.localnumber,
			missed.cidname,
			IF(next.datetime IS NULL, 360,
			LEAST(TIMESTAMPDIFF(MINUTE, missed.datetime, next.datetime), 360)) AS retrytime
		FROM
			(SELECT id, localnumber, cidname, datetime
			FROM xferfaxlog
			WHERE callMissed = 1
			ORDER BY datetime DESC
			LIMIT 2500) AS missed
		LEFT JOIN
			xferfaxlog AS next ON missed.localnumber = next.localnumber
			AND missed.cidname = next.cidname
			AND next.datetime > missed.datetime
			AND next.datetime <= DATE_ADD(missed.datetime, INTERVAL 360 MINUTE)
			AND next.callMissed = 0
		ORDER BY
			missed.datetime DESC;
			`

	// Execute the insert query
	result, err := db.Exec(insertMissedCalls)
	if err != nil {
		return "", fmt.Errorf("could not insert missed calls into missed_faxlog: %w", err)
	}

	// Check affected rows
	affectedRows, err := result.RowsAffected()
	if err != nil {
		return "", err
	}

	if affectedRows == 0 {
		return "No rows were inserted into missed_faxlog. Please check the data.", nil
	}

	return fmt.Sprintf("%d rows inserted into missed_faxlog", affectedRows), nil
}
