//Package couchdb provides a simple REST client for CouchDB
package couchdb

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

//Contains Authentication info: Username and password
type Auth struct{ Username, Password string }

type Connection struct{ *connection }

type Database struct {
	dbName     string
	connection *Connection
	auth       *Auth
}

//Creates a regular http connection.
//Timeout sets the timeout for the http Client
func NewConnection(address string, port int,
	timeout time.Duration) (*Connection, error) {

	url := "http://" + address + ":" + strconv.Itoa(port)
	return createConnection(url, timeout)
}

//Creates an https connection.
//Timeout sets the timeout for the http Client
func NewSSLConnection(address string, port int,
	timeout time.Duration) (*Connection, error) {

	url := "https://" + address + ":" + strconv.Itoa(port)
	return createConnection(url, timeout)
}

func createConnection(rawUrl string, timeout time.Duration) (*Connection, error) {
	//check that the url is valid
	theUrl, err := url.Parse(rawUrl)
	if err != nil {
		return nil, err
	}
	return &Connection{
		&connection{
			url:    theUrl.String(),
			client: &http.Client{Timeout: timeout},
		},
	}, nil

}

//Use to check if database server is alive.
func (conn *Connection) Ping() error {
	resp, err := conn.request("HEAD", "/", nil, nil, nil)
	if err == nil {
		resp.Body.Close()
	}
	return err
}

//DATABASES.
//Return a list of all databases on the server
func (conn *Connection) GetDBList() (dbList []string, err error) {
	resp, err := conn.request("GET", "/_all_dbs", nil, nil, nil)
	if err != nil {
		return dbList, err
	}
	err = parseBody(resp, &dbList)
	return dbList, err
}

//Create a new Database.
func (conn *Connection) CreateDB(name string, auth Auth) error {
	url, err := buildUrl(name)
	if err != nil {
		return err
	}
	resp, err := conn.request("PUT", url, nil, nil, &auth)
	if err == nil {
		resp.Body.Close()
	}
	return err
}

//Delete a Database.
func (conn *Connection) DeleteDB(name string, auth Auth) error {
	url, err := buildUrl(name)
	if err != nil {
		return err
	}
	resp, err := conn.request("DELETE", url, nil, nil, &auth)
	if err == nil {
		resp.Body.Close()
	}
	return err
}

//Add a User.
//This is a convenience method for adding a simple user to CouchDB.
//If you need a User with custom fields, etc., you'll just have to use the
//ordinary document methods on the "_users" database.
func (conn *Connection) AddUser(username string, password string,
	roles []string, auth Auth) (string, error) {

	userData := struct {
		Name     string   `json:"name"`
		Password string   `json:"password"`
		Roles    []string `json:"roles"`
		TheType  string   `json:"type"` //apparently type is a keyword in Go :)
	}{username, password, roles, "user"}

	userDb := conn.SelectDB("_users", auth)
	namestring := "org.couchdb.user:" + userData.Name
	return userDb.Save(userData, namestring, "")

}

//Delete a user.
func (conn *Connection) DeleteUser(username string, rev string, auth Auth) (string, error) {
	userDb := conn.SelectDB("_users", auth)
	namestring := "org.couchdb.user:" + username
	return userDb.Delete(namestring, rev)
}

//Select a Database.
//TODO: Perhaps verify dbName exists in couchdb?
//Or just do the fast thing here and let subsequent queries fail if the user supplies an incorrect dbname.
func (conn *Connection) SelectDB(dbName string, auth Auth) *Database {
	return &Database{
		dbName:     dbName,
		connection: conn,
		auth:       &Auth{auth.Username, auth.Password},
	}
}

//Returns the Username associated with this Database connection
func (db *Database) GetUsername() string {
	return db.auth.Username
}

//Save a document to the database.
//If you're creating a new document, pass an empty string for rev.
//If updating, you must specify the current rev.
//Returns the revision number assigned to the doc by CouchDB.
func (db *Database) Save(doc interface{}, id string, rev string) (string, error) {
	url, err := buildUrl(db.dbName, id)
	if err != nil {
		return "", err
	}
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
	resp, err := db.connection.request("PUT", url, data, headers, db.auth)
	if err != nil {
		return "", err
	}
	resp.Body.Close()
	return getRevInfo(resp)
}

//Copies a document into a new... document.
//Returns the revision of the newly created document
func (db *Database) Copy(fromId string, fromRev string, toId string) (string, error) {
	url, err := buildUrl(db.dbName, fromId)
	if err != nil {
		return "", err
	}
	var headers = make(map[string]string)
	headers["Accept"] = "application/json"
	if fromId == "" || toId == "" {
		return "", fmt.Errorf("Invalid request.  Ids must be specified")
	}
	if fromRev != "" {
		headers["If-Match"] = fromRev
	}
	headers["Destination"] = toId
	resp, err := db.connection.request("COPY", url, nil, headers, db.auth)
	if err != nil {
		return "", err
	}
	resp.Body.Close()
	return getRevInfo(resp)
}

//Fetches a document from the database.
//Pass it a &struct to hold the contents of the fetched document (doc).
//Returns the current revision and/or error
func (db *Database) Read(id string, doc interface{}, params *url.Values) (string, error) {
	var headers = make(map[string]string)
	headers["Accept"] = "application/json"
	var url string
	var err error
	if params == nil {
		url, err = buildUrl(db.dbName, id)
	} else {
		url, err = buildParamUrl(*params, db.dbName, id)
	}
	if err != nil {
		return "", err
	}
	resp, err := db.connection.request("GET", url, nil, headers, db.auth)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	err = parseBody(resp, &doc)
	if err != nil {
		return "", err
	}
	return getRevInfo(resp)
}

//Deletes a document.
//Or rather, tells CouchDB to mark the document as deleted.
//Yes, CouchDB will return a new revision, so this function returns it.
func (db *Database) Delete(id string, rev string) (string, error) {
	url, err := buildUrl(db.dbName, id)
	if err != nil {
		return "", err
	}
	var headers = make(map[string]string)
	headers["Accept"] = "application/json"
	headers["If-Match"] = rev
	resp, err := db.connection.request("DELETE", url, nil, headers, db.auth)
	if err != nil {
		return "", err
	}
	resp.Body.Close()
	return getRevInfo(resp)
}

//Saves an attachment.
//docId and docRev refer to the parent document.
//attType is the MIME type of the attachment (ex: image/jpeg) or some such.
//attContent is a byte array containing the actual content.
func (db *Database) SaveAttachment(docId string,
	docRev string, attName string,
	attType string, attContent []byte) (string, error) {
	url, err := buildUrl(db.dbName, docId, attName)
	if err != nil {
		return "", err
	}
	var headers = make(map[string]string)
	headers["Accept"] = "application/json"
	headers["Content-Type"] = attType
	headers["If-Match"] = docRev

	resp, err := db.connection.request("PUT", url, bytes.NewReader(attContent), headers, db.auth)
	if err != nil {
		return "", err
	}
	resp.Body.Close()
	return getRevInfo(resp)
}

//Gets an attachment.
//TODO: This is simplistic and won't work very well for larger attachments.
func (db *Database) GetAttachment(docId string, docRev string,
	attType string, attName string) ([]byte, error) {
	url, err := buildUrl(db.dbName, docId, attName)
	if err != nil {
		return nil, err
	}
	var headers = make(map[string]string)
	headers["Accept"] = attType
	if docRev != "" {
		headers["If-Match"] = docRev
	}
	resp, err := db.connection.request("GET", url, nil, headers, db.auth)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return data, nil
}

//Deletes an attachment
func (db *Database) DeleteAttachment(docId string, docRev string,
	attName string) (string, error) {
	url, err := buildUrl(db.dbName, docId, attName)
	if err != nil {
		return "", err
	}
	var headers = make(map[string]string)
	headers["Accept"] = "application/json"
	headers["If-Match"] = docRev
	resp, err := db.connection.request("DELETE", url, nil, headers, db.auth)
	if err != nil {
		return "", err
	}
	resp.Body.Close()
	return getRevInfo(resp)
}

type Members struct {
	Users []string `json:"users"`
	Roles []string `json:"roles"`
}

type Security struct {
	Members Members `json:"members"`
	Admins  Members `json:"admins"`
}

//Returns the Security document from the database.
func (db *Database) GetSecurity() (*Security, error) {
	url, err := buildUrl(db.dbName, "_security")
	if err != nil {
		return nil, err
	}
	var headers = make(map[string]string)
	sec := Security{}
	headers["Accept"] = "application/json"
	resp, err := db.connection.request("GET", url, nil, headers, db.auth)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	err = parseBody(resp, &sec)
	if err != nil {
		return nil, err
	}
	return &sec, err
}

//Save a security document to the database.
func (db *Database) SaveSecurity(sec Security) error {
	url, err := buildUrl(db.dbName, "_security")
	if err != nil {
		return err
	}
	var headers = make(map[string]string)
	headers["Accept"] = "application/json"
	data, err := encodeData(sec)
	if err != nil {
		return err
	}
	resp, err := db.connection.request("PUT", url, data, headers, db.auth)
	if err == nil {
		resp.Body.Close()
	}
	return err
}

//Get the results of a view.
func (db *Database) GetView(designDoc string, view string,
	results interface{}, params *url.Values) error {
	var err error
	var url string
	if params == nil {
		url, err = buildUrl(db.dbName, "_design", designDoc, "_view", view)
	} else {
		url, err = buildParamUrl(*params, db.dbName, "_design",
			designDoc, "_view", view)
	}
	if err != nil {
		return err
	}
	var headers = make(map[string]string)
	headers["Accept"] = "application/json"
	resp, err := db.connection.request("GET", url, nil, headers, db.auth)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	err = parseBody(resp, &results)
	if err != nil {
		return err
	}
	return nil
}

//Save a design document.
//If creating a new design doc, set rev to "".
func (db *Database) SaveDesignDoc(name string,
	designDoc interface{}, rev string) (string, error) {
	path := "_design/" + name
	newRev, err := db.Save(designDoc, path, rev)
	if err != nil {
		return "", err
	} else if newRev == "" {
		return "", fmt.Errorf("CouchDB returned an empty revision string.")
	}
	return newRev, nil

}
