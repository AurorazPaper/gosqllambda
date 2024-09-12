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

type inboundColumns struct {
	starttime    string
	system       string
	commid       string
	FaxFrom      string
	FaxTo        string
	Conntime     string
	npages       string
	entrytype    string
	cidname      string
	tsi          string
	reason       string
	receivedfile string
}

type columnTransferInfo struct {
	originTable       string
	originColumn      string
	destinationTable  string
	destinationColumn string
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

	// Define the SQL statement to create a new table
	createTableSQL := `
		CREATE TABLE IF NOT EXISTS inboundReportTable (
			id INT AUTO_INCREMENT PRIMARY KEY,
			,
			email VARCHAR(100),
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);
		`

	// Execute the SQL statement
	_, err = db.Exec(createTableSQL)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Table created successfully!")

	var inboundTransferInfo columnTransferInfo

	inboundTransferInfo.originTable = "xferfaxlog"
	inboundTransferInfo.originColumn = ""
	inboundTransferInfo.destinationTable = "inboundReportTable"
	inboundTransferInfo.destinationColumn = ""

	return "Successfully connected to RDS", nil

}

func main() {

	lambda.Start(HandleRequestTest)

}

func queryfaxrecords(recordtype string) (interface{}, error) {
	var faxlogs []faxLog

	rows, err := db.Query("SELECT datetime, qfile, localnumber, tsi, npages, cidname, cidnumber, jobinfo FROM xferfaxlog WHERE entrytype = ? LIMIT 5", recordtype)
	if err != nil {
		return nil, fmt.Errorf("queryfaxrecords %q: %v", recordtype, err)
	}
	defer rows.Close()
	// Loop through rows, using Scan to assign data from xferfaxlogs to struct fields
	for rows.Next() {
		var log faxLog

		log.datetime = ""
		log.entrytype = ""
		log.qfile = ""
		log.commid = ""
		log.localnumber = ""
		log.tsi = ""
		log.conntime = ""
		log.cidname = ""
		log.cidnumber = ""
		log.jobinfo = ""
		log.system = ""

		if err := rows.Scan(&log.datetime, &log.qfile, &log.localnumber, &log.tsi, &log.npages, &log.cidname, &log.cidnumber, &log.jobinfo); err != nil {
			return nil, fmt.Errorf("queryfaxrecords %q: %v", recordtype, err)
		}
		faxlogs = append(faxlogs, log)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("queryfaxrecords %q: %v", recordtype, err)
	}
	return faxlogs, nil

}

func faxColumnTransfer(tableName string, oldColumnName string, newColumnName string) (string, error) {
	// Fetch data from source table
	rows, err := db.Query("SELECT old_column_name FROM source_table")
	if err != nil {
		return "", fmt.Errorf("failed to query source table: %w", err)
	}
	defer rows.Close()

	// Prepare insert statement for target table
	stmt, err := db.Prepare("INSERT INTO target_table (new_column_name) VALUES (?)")
	if err != nil {
		return "", fmt.Errorf("failed to prepare insert statement: %w", err)
	}
	defer stmt.Close()

	// Process and insert data
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			return "", fmt.Errorf("failed to scan row: %w", err)
		}

		if _, err := stmt.Exec(value); err != nil {
			return "", fmt.Errorf("failed to insert data into target table: %w", err)
		}
	}

	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("error occurred during rows iteration: %w", err)
	}

	return "Data transfer completed successfully", nil

}
