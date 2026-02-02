package db

import (
	"database/sql"
	"fmt"
	"os"
	// "github.com/lib/pq"
)

var connStr string

// init() initializes the PostgreSQL connection string from environment variables.
// We also fallback to defaults if nothing is set.
func init() {
	// just assume localhost if not set
	host := os.Getenv("PG_HOST")
	if host == "" {
		host = "localhost"
	}

	// PostgreSQL defaults to port 5432
	port := os.Getenv("PG_PORT")
	if port == "" {
		port = "5432"
	}

	user := os.Getenv("PG_USER")
	if user == "" {
		user = "postgres"
	}

	// PostgreSQL defaults to no password for some reason. :/
	password := os.Getenv("PG_PASSWORD")

	dbname := os.Getenv("PG_DATABASE")
	if dbname == "" {
		dbname = "stultus"
	}

	// Form th connection string
	connStr = fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s", host, port, user, password, dbname)
}

func NewConn() (*sql.DB, error) {
	return sql.Open("postgres", connStr)
	// will want error handling, and stuff for connection limits, etc etc
}
