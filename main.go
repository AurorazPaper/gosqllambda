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

type GoTestEvent struct {
	Name string `json:"name"`
}

// Make sure to set environment variables to linux and arm64
// GOOS=linux GOARCH=arm64

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

	// No need for WaitGroup in linear execution

	// Process missed calls
	if _, err := processMissedcalls(db); err != nil {
		log.Println("Failure to process missed calls:", err)
	}

	// Process incomplete faxes
	if _, err := processIncompleteFax(db); err != nil {
		log.Println("Failure to process incomplete faxes:", err)
	}

	// Process call difference logic
	if _, err := missedCallDiff(db); err != nil {
		log.Println("Failure to process call difference logic:", err)
	}

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

// Processes fax as complete or incomplete, leaves faxincomplete null if entrytype isn't RECV
func processIncompleteFax(db *sql.DB) (string, error) {
	updateCallMissed :=
		`UPDATE xferfaxlog AS missed
		SET faxincomplete = 
		  CASE
		    WHEN entrytype = 'RECV' AND npages != 0 AND reason != '' THEN 1
		    WHEN entrytype = 'RECV' AND (npages = 0 OR reason = '') THEN 0
			ELSE faxincomplete
		END;
		`

	_, err := db.Exec(updateCallMissed)
	if err != nil {
		return "", fmt.Errorf("could not run incomplete fax logic: %w", err)
	}

	return "incomplete fax processing successful", nil

}

// This solution only works in a static database
// In a live database, entries younger than 6 hours should be set to null rather than 360
// This can be done by using the NOW and DATE_SUB functions
// All entries where retrytime is NULL and entrytype is CALL can be counted as missed calls
func missedCallDiff(db *sql.DB) (string, error) {
	insertMissedCalls := `
        UPDATE theBigTable AS missed
		JOIN (
			SELECT 
				missed.id,
				COALESCE( # Replace with CASE for live DB
					TIMESTAMPDIFF(MINUTE, missed.datetime, 
						(SELECT MIN(next.datetime) 
						FROM theBigTable next 
						WHERE next.localnumber = missed.localnumber 
						AND next.cidname = missed.cidname 
						AND next.datetime > missed.datetime 
						AND next.datetime <= DATE_ADD(missed.datetime, INTERVAL 360 MINUTE)
						AND next.callMissed = 0)
					), 
					360
				) AS retrytime
			FROM 
				theBigTable missed
			WHERE 
				missed.callMissed = 1 AND retrytime IS NULL
			ORDER BY 
				missed.datetime DESC
			LIMIT 2500
		) AS retry ON missed.id = retry.id
		SET missed.retrytime = retry.retrytime;
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
