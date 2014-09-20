package couchdb_test

import (
	"github.com/rhinoman/couchdb-go"
	"github.com/twinj/uuid"
	"strconv"
	"testing"
	"time"
)

var timeout = time.Duration(500 * time.Millisecond)
var unittestdb = "unittestdb"
var server = "127.0.0.1"
var numDbs = 1

type TestDocument struct {
	Title string
	Note  string
}

func getUuid() string {
	theUuid := uuid.NewV4()
	return uuid.Formatter(theUuid, uuid.Clean)
}

func getConnection(t *testing.T) *couchdb.Connection {
	conn, err := couchdb.NewConnection(server, 5984, couchdb.Auth{}, timeout)
	if err != nil {
		t.Logf("ERROR: %v", err)
		t.Fail()
	}
	return conn
}

func getAuthConnection(t *testing.T) *couchdb.Connection {
	auth := couchdb.Auth{Username: "adminuser", Password: "password"}
	conn, err := couchdb.NewConnection(server, 5984, auth, timeout)
	if err != nil {
		t.Logf("ERROR: %v", err)
		t.Fail()
	}
	return conn
}

func createTestDb(t *testing.T) string {
	conn := getAuthConnection(t)
	dbName := unittestdb + strconv.Itoa(numDbs)
	err := conn.CreateDB(dbName)
	errorify(t, err)
	numDbs += 1
	return dbName
}

func deleteTestDb(t *testing.T, dbName string) {
	conn := getAuthConnection(t)
	err := conn.DeleteDB(dbName)
	errorify(t, err)
}

func errorify(t *testing.T, err error) {
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
	conn, err := couchdb.NewConnection("unpingable", 1234, couchdb.Auth{}, timeout)
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
			t.Logf("Database %v: %v\n", i, dbName)
		}
	}
}

func TestCreateDB(t *testing.T) {
	conn := getAuthConnection(t)
	err := conn.CreateDB("testcreatedb")
	errorify(t, err)
	//try to create it again --- should fail
	err = conn.CreateDB("testcreatedb")
	if err == nil {
		t.Fail()
	}
	//now delete it
	err = conn.DeleteDB("testcreatedb")
	errorify(t, err)
}

func TestSave(t *testing.T) {
	dbName := createTestDb(t)
	conn := getConnection(t)
	//Create a new document
	theDoc := TestDocument{
		Title: "My Document",
		Note:  "This is my note",
	}
	db := conn.SelectDB(dbName)
	theId := getUuid()
	//Save it
	t.Logf("Saving first\n")
	rev, err := db.Save(theDoc, theId, "")
	errorify(t, err)
	t.Logf("New Document ID: %s\n", theId)
	t.Logf("New Document Rev: %s\n", rev)
	t.Logf("New Document Title: %v\n", theDoc.Title)
	t.Logf("New Document Note: %v\n", theDoc.Note)
	if theDoc.Title != "My Document" ||
		theDoc.Note != "This is my note" || rev == "" {
		t.Fail()
	}
	//Now, let's try updating it
	theDoc.Note = "A new note"
	t.Logf("Saving again\n")
	rev, err = db.Save(theDoc, theId, rev)
	errorify(t, err)
	t.Logf("Updated Document Id: %s\n", theId)
	t.Logf("Updated Document Rev: %s\n", rev)
	t.Logf("Updated Document Title: %v\n", theDoc.Title)
	t.Logf("Updated Document Note: %v\n", theDoc.Note)
	if theDoc.Note != "A new note" {
		t.Fail()
	}
	deleteTestDb(t, dbName)
}

func TestRead(t *testing.T) {
	dbName := createTestDb(t)
	conn := getConnection(t)
	db := conn.SelectDB(dbName)
	//Create a test doc
	theDoc := TestDocument{
		Title: "My Document",
		Note:  "Time to read",
	}
	emptyDoc := TestDocument{}
	//Save it
	theId := getUuid()
	_, err := db.Save(theDoc, theId, "")
	errorify(t, err)
	//Now try to read it
	rev, err := db.Read(theId, &emptyDoc)
	errorify(t, err)
	t.Logf("Document Id: %v\n", theId)
	t.Logf("Document Rev: %v\n", rev)
	t.Logf("Document Title: %v\n", emptyDoc.Title)
	t.Logf("Document Note: %v\n", emptyDoc.Note)
	deleteTestDb(t, dbName)
}

func TestDelete(t *testing.T) {
	dbName := createTestDb(t)
	conn := getConnection(t)
	db := conn.SelectDB(dbName)
	//Create a test doc
	theDoc := TestDocument{
		Title: "My Document",
		Note:  "Time to read",
	}
	theId := getUuid()
	rev, err := db.Save(theDoc, theId, "")
	errorify(t, err)
	//Now delete it
	newRev, err := db.Delete(theId, rev)
	errorify(t, err)
	t.Logf("Document Id: %v\n", theId)
	t.Logf("Document Rev: %v\n", rev)
	t.Logf("Deleted Rev: %v\n", newRev)
	if newRev == "" || newRev == rev {
		t.Fail()
	}
	deleteTestDb(t, dbName)
}

func TestAddUser(t *testing.T) {
	conn := getAuthConnection(t)
	//Save a User
	rev, err := conn.AddUser("turd.ferguson",
		"passw0rd", []string{"loser"})
	errorify(t, err)
	t.Logf("User Rev: %v\n", rev)
	if rev == "" {
		t.Fail()
	}
	dRev, err := conn.DeleteUser("turd.ferguson", rev)
	errorify(t, err)
	t.Logf("Del User Rev: %v\n", dRev)
	if rev == dRev || dRev == "" {
		t.Fail()
	}
}

func TestSecurity(t *testing.T) {
	conn := getAuthConnection(t)
	dbName := createTestDb(t)
	db := conn.SelectDB(dbName)

	members := couchdb.Members{
		Users: []string{"joe, bill"},
		Roles: []string{"code monkeys"},
	}
	admins := couchdb.Members{
		Users: []string{"bossman"},
		Roles: []string{"boss"},
	}
	security := couchdb.Security{
		Members: members,
		Admins:  admins,
	}
	err := db.SaveSecurity(security)
	errorify(t, err)
	sec, err := db.GetSecurity()
	t.Logf("Security: %v\n", sec)
	if sec.Admins.Users[0] != "bossman" {
		t.Fail()
	}
	if sec.Admins.Roles[0] != "boss" {
		t.Fail()
	}
	errorify(t, err)
	deleteTestDb(t, dbName)
}
