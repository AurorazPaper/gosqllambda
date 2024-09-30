package main

import (
	"database/sql"
	"fmt"
)

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
