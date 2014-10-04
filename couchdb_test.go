package couchdb_test

import (
	"bytes"
	"github.com/rhinoman/couchdb-go"
	"github.com/twinj/uuid"
	"io/ioutil"
	"strconv"
	"testing"
	"time"
)

var timeout = time.Duration(500 * time.Millisecond)
var unittestdb = "unittestdb"
var server = "127.0.0.1"
var numDbs = 1
var adminAuth = couchdb.Auth{Username: "adminuser", Password: "password"}

type TestDocument struct {
	Title string
	Note  string
}

type ViewResult struct {
	Id  string       `json:"id"`
	Key TestDocument `json:"key"`
}

type ViewResponse struct {
	TotalRows int          `json:"total_rows"`
	Offset    int          `json:"offset"`
	Rows      []ViewResult `json:"rows,omitempty"`
}

type View struct {
	Map    string `json:"map"`
	Reduce string `json:"reduce,omitempty"`
}

type DesignDocument struct {
	Language string          `json:"language"`
	Views    map[string]View `json:"views"`
}

func getUuid() string {
	theUuid := uuid.NewV4()
	return uuid.Formatter(theUuid, uuid.Clean)
}

func getConnection(t *testing.T) *couchdb.Connection {
	conn, err := couchdb.NewConnection(server, 5984, timeout)
	if err != nil {
		t.Logf("ERROR: %v", err)
		t.Fail()
	}
	return conn
}

/*func getAuthConnection(t *testing.T) *couchdb.Connection {
	auth := couchdb.Auth{Username: "adminuser", Password: "password"}
	conn, err := couchdb.NewConnection(server, 5984, timeout)
	if err != nil {
		t.Logf("ERROR: %v", err)
		t.Fail()
	}
	return conn
}*/

func createTestDb(t *testing.T) string {
	conn := getConnection(t)
	dbName := unittestdb + strconv.Itoa(numDbs)
	err := conn.CreateDB(dbName, adminAuth)
	errorify(t, err)
	numDbs += 1
	return dbName
}

func deleteTestDb(t *testing.T, dbName string) {
	conn := getConnection(t)
	err := conn.DeleteDB(dbName, adminAuth)
	errorify(t, err)
}

func createLotsDocs(t *testing.T, db *couchdb.Database) {
	for i := 0; i < 10; i++ {
		id := getUuid()
		note := "purple"
		if i%2 == 0 {
			note = "magenta"
		}
		testDoc := TestDocument{
			Title: "TheDoc -- " + strconv.Itoa(i),
			Note:  note,
		}
		_, err := db.Save(testDoc, id, "")
		errorify(t, err)
	}
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
	conn, err := couchdb.NewConnection("unpingable", 1234, timeout)
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
	conn := getConnection(t)
	err := conn.CreateDB("testcreatedb", adminAuth)
	errorify(t, err)
	//try to create it again --- should fail
	err = conn.CreateDB("testcreatedb", adminAuth)
	if err == nil {
		t.Fail()
	}
	//now delete it
	err = conn.DeleteDB("testcreatedb", adminAuth)
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
	db := conn.SelectDB(dbName, couchdb.Auth{})
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

func TestAttachment(t *testing.T) {
	dbName := createTestDb(t)
	conn := getConnection(t)
	//Create a new document
	theDoc := TestDocument{
		Title: "My Document",
		Note:  "This one has attachments",
	}
	db := conn.SelectDB(dbName, couchdb.Auth{})
	theId := getUuid()
	//Save it
	t.Logf("Saving document\n")
	rev, err := db.Save(theDoc, theId, "")
	errorify(t, err)
	t.Logf("New Document Id: %s\n", theId)
	t.Logf("New Document Rev: %s\n", rev)
	t.Logf("New Document Title: %v\n", theDoc.Title)
	t.Logf("New Document Note: %v\n", theDoc.Note)
	//Create some content
	content := []byte("This is my attachment")
	contentReader := bytes.NewReader(content)
	//Now Add an attachment
	uRev, err := db.SaveAttachment(theId, rev, "attachment", "text/plain", contentReader)
	errorify(t, err)
	t.Logf("Updated Rev: %s\n", uRev)
	//Now try to read it
	theContent, err := db.GetAttachment(theId, uRev, "text/plain", "attachment")
	errorify(t, err)
	defer theContent.Close()
	theBytes, err := ioutil.ReadAll(theContent)
	errorify(t, err)
	t.Logf("how much data: %v\n", len(theBytes))
	data := string(theBytes[:])
	if data != "This is my attachment" {
		t.Fail()
	}
	t.Logf("The data: %v\n", data)
	//Now delete it
	dRev, err := db.DeleteAttachment(theId, uRev, "attachment")
	errorify(t, err)
	t.Logf("Deleted revision: %v\n", dRev)
	deleteTestDb(t, dbName)
}

func TestRead(t *testing.T) {
	dbName := createTestDb(t)
	conn := getConnection(t)
	db := conn.SelectDB(dbName, couchdb.Auth{})
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
	rev, err := db.Read(theId, &emptyDoc, nil)
	errorify(t, err)
	t.Logf("Document Id: %v\n", theId)
	t.Logf("Document Rev: %v\n", rev)
	t.Logf("Document Title: %v\n", emptyDoc.Title)
	t.Logf("Document Note: %v\n", emptyDoc.Note)
	deleteTestDb(t, dbName)
}

func TestCopy(t *testing.T) {
	dbName := createTestDb(t)
	conn := getConnection(t)
	db := conn.SelectDB(dbName, couchdb.Auth{})
	//Create a test doc
	theDoc := TestDocument{
		Title: "My Document",
		Note:  "Time to read",
	}
	emptyDoc := TestDocument{}
	//Save it
	theId := getUuid()
	rev, err := db.Save(theDoc, theId, "")
	errorify(t, err)
	//Now copy it
	copyId := getUuid()
	copyRev, err := db.Copy(theId, "", copyId)
	errorify(t, err)
	t.Logf("Document Id: %v\n", theId)
	t.Logf("Document Rev: %v\n", rev)
	//Now read the copy
	_, err = db.Read(copyId, &emptyDoc, nil)
	errorify(t, err)
	t.Logf("Document Title: %v\n", emptyDoc.Title)
	t.Logf("Document Note: %v\n", emptyDoc.Note)
	t.Logf("Copied Doc Rev: %v\n", copyRev)
	deleteTestDb(t, dbName)
}

func TestDelete(t *testing.T) {
	dbName := createTestDb(t)
	conn := getConnection(t)
	db := conn.SelectDB(dbName, couchdb.Auth{})
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
	conn := getConnection(t)
	//Save a User
	rev, err := conn.AddUser("turd.ferguson",
		"passw0rd", []string{"loser"}, adminAuth)
	errorify(t, err)
	t.Logf("User Rev: %v\n", rev)
	if rev == "" {
		t.Fail()
	}
	dRev, err := conn.DeleteUser("turd.ferguson", rev, adminAuth)
	errorify(t, err)
	t.Logf("Del User Rev: %v\n", dRev)
	if rev == dRev || dRev == "" {
		t.Fail()
	}
}

func TestSecurity(t *testing.T) {
	conn := getConnection(t)
	dbName := createTestDb(t)
	db := conn.SelectDB(dbName, adminAuth)

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

func TestDesignDocs(t *testing.T) {
	conn := getConnection(t)
	dbName := createTestDb(t)
	db := conn.SelectDB(dbName, adminAuth)
	createLotsDocs(t, db)

	view := View{
		Map: "function(doc) {\n  if (doc.Note === \"magenta\"){\n    emit(doc)\n  }\n}",
	}
	views := make(map[string]View)
	views["find_all_magenta"] = view
	ddoc := DesignDocument{
		Language: "javascript",
		Views:    views,
	}
	rev, err := db.SaveDesignDoc("colors", ddoc, "")
	errorify(t, err)
	if rev == "" {
		t.Fail()
	} else {
		t.Logf("Rev of design doc: %v\n", rev)
	}
	result := ViewResponse{}
	//now try to query the view
	err = db.GetView("colors", "find_all_magenta", &result, nil)
	errorify(t, err)
	if len(result.Rows) != 5 {
		t.Logf("docList length: %v\n", len(result.Rows))
		t.Fail()
	} else {
		t.Logf("Results: %v\n", result.Rows)
	}

	deleteTestDb(t, dbName)

}
