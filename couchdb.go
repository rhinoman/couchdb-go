package couchdb

//File: couchdb.go
//Description: CouchDB driver

import (
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"
	"github.com/twinj/uuid"
)

type Auth struct{ Username, Password string }

type Connection struct {
	*connection
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
	resp, err := conn.request("HEAD", "/", nil)
	if err == nil {
		resp.Body.Close()
	}
	return err
}

//DATABASES
//Return a list of all databases on the server
func (conn *Connection) GetDBList() (dbList []string, err error) {
	resp, err := conn.request("GET", "/_all_dbs", nil)
	if err != nil {
		return dbList, err
	}
	err = parseBody(resp, &dbList)
	return dbList, err
}

//Create a new Database
func (conn *Connection) CreateDB (name string) error {
	resp, err := conn.request("PUT", cleanPath(name), nil)
	if err == nil {
		resp.Body.Close()
	}
	return err
}

//Delete a Database
func (conn *Connection) DeleteDB (name string) error {
	resp, err := conn.request("DELETE", cleanPath(name), nil)
	if err == nil {
		resp.Body.Close()
	}
	return err
}

//DOCUMENTS

//Create a new document. 
//returns the id and rev of the newly created document
func (conn *Connection) CreateDoc (dbName string,
	doc interface{})(id string, rev string, err error) {
	id = uuid.Formatter(uuid.NewV4(),uuid.Clean)
	data, err := encodeData(doc)
	if err != nil {
		return "","", err
	}
	resp, err := conn.request("PUT", cleanPath(dbName, id), data)
	if err != nil {
		return "","",err
	} else if rev = resp.Header.Get("ETag"); rev == ""{
			return "","",fmt.Errorf("Bad response from CouchDB")
	} else {
		rev = rev[1:len(rev)-1] //remove the "" from the ETag
		parseBody(resp, &doc)
		return id, rev, nil
	}

}

