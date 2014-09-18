package couchdb

//File: couchdb.go
//Description: CouchDB driver

import (
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

type Auth struct{ Username, Password string }

type Connection struct {
	*connection
}

type Database struct {
	dbName     string
	connection *Connection
}

type Document struct {
	Id  string `structs:",omitempty"`
	Rev string `structs:",omitempty"`
}

//creates a regular http connection
//timeout sets the timeout for the http Client
func NewConnection(address string, port int,
	auth Auth, timeout time.Duration) (*Connection, error) {

	url := "http://" + address + ":" + strconv.Itoa(port)
	return createConnection(url, auth, timeout)
}

//creates an https connection
//timeout sets the timeout for the http Client
func NewSSLConnection(address string, port int,
	auth Auth, timeout time.Duration) (*Connection, error) {

	url := "https://" + address + ":" + strconv.Itoa(port)
	return createConnection(url, auth, timeout)
}

func createConnection(rawUrl string, auth Auth, timeout time.Duration) (*Connection, error) {
	//check that the url is valid
	theUrl, err := url.Parse(rawUrl)
	if err != nil {
		return nil, err
	}
	return &Connection{
		&connection{
			url:      theUrl.String(),
			client:   &http.Client{Timeout: timeout},
			username: auth.Username,
			password: auth.Password,
		},
	}, nil

}

//Use to check if database server is alive
func (conn *Connection) Ping() error {
	resp, err := conn.request("HEAD", "/", nil, nil)
	if err == nil {
		resp.Body.Close()
	}
	return err
}

//DATABASES
//Return a list of all databases on the server
func (conn *Connection) GetDBList() (dbList []string, err error) {
	resp, err := conn.request("GET", "/_all_dbs", nil, nil)
	if err != nil {
		return dbList, err
	}
	err = parseBody(resp, &dbList)
	return dbList, err
}

//Create a new Database
func (conn *Connection) CreateDB(name string) error {
	resp, err := conn.request("PUT", cleanPath(name), nil, nil)
	if err == nil {
		resp.Body.Close()
	}
	return err
}

//Delete a Database
func (conn *Connection) DeleteDB(name string) error {
	resp, err := conn.request("DELETE", cleanPath(name), nil, nil)
	if err == nil {
		resp.Body.Close()
	}
	return err
}

//Select a Database
//TODO: Perhaps verify dbName exists in couchdb?
//Or just do the fast thing here and let subsequent queries fail
//if the user supplies an incorrect dbname
func (conn *Connection) SelectDB(dbName string) *Database {
	return &Database{
		dbName:     dbName,
		connection: conn,
	}
}

//DOCUMENTS

//Save a document to the database
//If you're creating a new document, pass an empty string for rev
//If updating, you must specify the current rev
func (db *Database) Save(doc interface{}, id string, rev string) (string, error) {
	var headers = make(map[string]string)
	headers["Content-Type"] = "application/json"
	headers["Accept"] = "application/json"
	if id == "" {
		return "", fmt.Errorf("No ID specified")
	}
	if rev != "" {
		headers["If-Match"] = rev
	}
	data, err := encodeData(doc)
	if err != nil {
		return "", err
	}
	resp, err := db.connection.request("PUT", cleanPath(db.dbName, id), data, headers)
	if err != nil {
		return "", err
	} else if rev := resp.Header.Get("ETag"); rev == "" {
		resp.Body.Close()
		return "", fmt.Errorf("Bad response from CouchDB")
	} else {
		resp.Body.Close()
		rev = rev[1 : len(rev)-1] //remove the "" from the ETag
		return rev, nil
	}
}

//Fetches a document from the database
//pass it a &struct to hold the contents of the fetched document (doc)
//returns the current revision and/or error
func (db *Database) Read(id string, doc interface{}) (string, error) {
	var headers = make(map[string]string)
	headers["Accept"] = "application/json"
	resp, err := db.connection.request("GET", cleanPath(db.dbName, id), nil, headers)
	if err != nil {
		return "", err
	}
	err = parseBody(resp, &doc)
	if err != nil {
		resp.Body.Close()
		return "", err
	} else if rev := resp.Header.Get("ETag"); rev == "" {
		resp.Body.Close()
		return "", fmt.Errorf("Bad response from CouchDB")
	} else {
		resp.Body.Close()
		rev = rev[1 : len(rev)-1]
		return rev, nil
	}

}

//Deletes a document 
//Or rather, tells CouchDB to mark the document as deleted
//Yes, CouchDB will return a new revision, so this function returns it
func (db *Database) Delete(id string, rev string) (string, error) {
	var headers = make(map[string]string)
	headers["Accept"] = "application/json"
	headers["If-Match"] = rev
	resp, err := db.connection.request("DELETE", cleanPath(db.dbName, id), nil, headers)
	if err != nil {
		return "", err
	} else if newRev := resp.Header.Get("Etag"); newRev == "" {
		resp.Body.Close()
		return "", fmt.Errorf("Bad response from CouchDB")
	} else {
		resp.Body.Close()
		newRev = newRev[1 : len(rev)-1]
		return newRev, nil
	}
}
