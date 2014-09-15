package couchdb

//File: couchdb.go
//Description: CouchDB driver

import (
	"net/http"
	"net/url"
	"strconv"
	"time"
)

type Auth struct{ username, password string }

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
			username: auth.username,
			password: auth.password,
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

//Return a list of all databases on the server
func (conn *Connection) AllDBs() (dbList []string, err error) {
	resp, err := conn.request("GET", "/_all_dbs", nil)
	if err != nil {
		return dbList, err
	}
	err = parseBody(resp, &dbList)
	return dbList, err
}

