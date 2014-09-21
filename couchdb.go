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

type Connection struct{ *connection }

type Database struct {
	dbName     string
	connection *Connection
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

//Add a User
//This is a convenience method for adding a simple user to CouchDB.
//If you need a User with custom fields, etc., you'll just have to use the
//ordinary document methods on the "_users" database
func (conn *Connection) AddUser(username string, password string,
	roles []string) (string, error) {

	userData := struct {
		Name     string   `json:"name"`
		Password string   `json:"password"`
		Roles    []string `json:"roles"`
		TheType  string   `json:"type"` //apparently type is a keyword in Go :)
	}{username, password, roles, "user"}

	userDb := conn.SelectDB("_users")
	namestring := "org.couchdb.user:" + userData.Name
	return userDb.Save(userData, namestring, "")

}

//Delete a user
func (conn *Connection) DeleteUser(username string, rev string) (string, error) {
	userDb := conn.SelectDB("_users")
	namestring := "org.couchdb.user:" + username
	return userDb.Delete(namestring, rev)
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
//returns the revision number assigned to the doc by CouchDB
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
	}
	resp.Body.Close()
	return getRevInfo(resp)
}

//Fetches a document from the database
//pass it a &struct to hold the contents of the fetched document (doc)
//returns the current revision and/or error
//TODO: Add ability to pass query args to CouchDB
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
	}
	resp.Body.Close()
	return getRevInfo(resp)
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
	}
	resp.Body.Close()
	return getRevInfo(resp)
}

//Database security
type Members struct {
	Users []string `json:"users"`
	Roles []string `json:"roles"`
}

type Security struct {
	Members Members `json:"members"`
	Admins  Members `json:"admins"`
}

//Returns the Security document from the database
func (db *Database) GetSecurity() (*Security, error) {
	var headers = make(map[string]string)
	sec := Security{}
	headers["Accept"] = "application/json"
	resp, err := db.connection.request("GET",
		cleanPath(db.dbName, "_security"), nil, headers)
	if err != nil {
		return nil, err
	}
	err = parseBody(resp, &sec)
	if err != nil {
		resp.Body.Close()
		return nil, err
	}
	resp.Body.Close()
	return &sec, err
}

//Save a security document to the database
func (db *Database) SaveSecurity(sec Security) error {
	var headers = make(map[string]string)
	headers["Accept"] = "application/json"
	data, err := encodeData(sec)
	if err != nil {
		return err
	}
	resp, err := db.connection.request("PUT",
		cleanPath(db.dbName, "_security"), data, headers)
	if err == nil {
		resp.Body.Close()
	}
	return err
}

//Get the results of a view
//TODO: Add query arguments / options
func (db *Database) GetView(designDoc string, view string,
	results interface{}) error {
	var headers = make(map[string]string)
	headers["Accept"] = "application/json"
	resp, err := db.connection.request("GET",
		cleanPath(db.dbName, "_design", designDoc, "_view", view),
			nil, headers)
	if err != nil {
		return err
	}
	err = parseBody(resp, &results)
	if err != nil {
		resp.Body.Close()
		return err
	}
	resp.Body.Close()
	return nil
}

//Save a design document
//If creating a new design doc, set rev to ""
func (db *Database) SaveDesignDoc(name string,
	designDoc interface{}, rev string) (string, error) {
	path := "_design/" + name
	newRev, err := db.Save(designDoc, path, rev)
	if err != nil{
		return "", err
	} else if newRev == ""{
		return "", fmt.Errorf("CouchDB returned an empty revision string.")
	}
	return newRev, nil

}
