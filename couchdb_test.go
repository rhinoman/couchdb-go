package couchdb_test

import (
	. "github.com/rhinoman/couchdb-go"
	"testing"
	"time"
)

var timeout = time.Duration(500 * time.Millisecond)

func getConnection(t *testing.T) *Connection {
	conn, err := NewConnection("maui-test", 5984, Auth{}, timeout)
	if err != nil {
		t.Logf("ERROR: %v", err)
		t.Fail()
	}
	return conn
}
func errorify(t *testing.T, err error){
	if err != nil {
		t.Logf("ERROR: %v", err)
		t.Fail()
	}
}

func TestPing(t *testing.T) {
	conn := getConnection(t)
	pingErr := conn.Ping()
	errorify(t, pingErr)
}

func TestBadPing(t *testing.T) {
	conn, err := NewConnection("unpingable", 1234, Auth{}, timeout)
	errorify(t, err)
	pingErr := conn.Ping()
	if pingErr == nil {
		t.Fail()
	}
}

func TestAllDBs(t *testing.T) {
	conn := getConnection(t)
	dbList, err := conn.AllDBs()
	errorify(t, err)
	if len(dbList) <= 0 {
		t.Logf("No results!")
		t.Fail()
	} else {
		for i, dbName := range dbList {
			t.Logf("Database %v: %v\n",i,dbName)
		}
	}
}
