package couchdb_test

import (
	. "github.com/rhinoman/couchdb-go"
	"testing"
	"time"
)

var timeout = time.Duration(500 * time.Millisecond)
var unittestdb = "unittestdb"
var server = "maui-test"

type TestDocument struct {
	Title	string
	Note	string
}

func getConnection(t *testing.T) *Connection {
	conn, err := NewConnection(server, 5984, Auth{}, timeout)
	if err != nil {
		t.Logf("ERROR: %v", err)
		t.Fail()
	}
	return conn
}

func getAuthConnection(t *testing.T) *Connection {
	auth := Auth{Username: "adminuser", Password: "password"}
	conn, err := NewConnection(server, 5984, auth, timeout)
	if err != nil {
		t.Logf("ERROR: %v", err)
		t.Fail()
	}
	return conn
}

func createTestDb(t *testing.T) string {
	conn := getAuthConnection(t)
	err := conn.CreateDB(unittestdb)
	errorify(t, err)
	return unittestdb
}

func deleteTestDb(t *testing.T) {
	conn := getAuthConnection(t)
	err := conn.DeleteDB(unittestdb)
	errorify(t, err)
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

func TestGetDBList(t *testing.T) {
	conn := getConnection(t)
	dbList, err := conn.GetDBList()
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

func TestCreateDB(t *testing.T){
	conn := getAuthConnection(t)
	err := conn.CreateDB("unittestdb")
	errorify(t, err)
	//try to create it again --- should fail
	err = conn.CreateDB("unittestdb")
	if err == nil{
		t.Fail()
	}
	//now delete it
	err = conn.DeleteDB("unittestdb")
	errorify(t,err)
}

func TestCreateDoc(t *testing.T){
	dbName := createTestDb(t)
	conn := getConnection(t)
	theDoc := TestDocument{
		Title: "My Document",
		Note: "This is my note",
	}
	id, rev, err := conn.CreateDoc(dbName, theDoc)
	errorify(t, err)
	t.Logf("New Document ID: %s\n", id)
	t.Logf("New Document Rev: %s\n", rev)
	deleteTestDb(t)
}
