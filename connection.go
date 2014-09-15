package couchdb

//File: connection.go
//Description: Lower level stuff happens here, should not be used directly

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
)

//represents a couchdb 'connection'
type connection struct {
	url      string
	client   *http.Client
	username string
	password string
}

//Adds HTTP Basic Authentication headers to a request
func addBasicAuthHeaders(username string, password string, req *http.Request) {
	authString := []byte(username + ":" + password)
	header := "Basic " + base64.StdEncoding.EncodeToString(authString)
	req.Header.Set("Authorization", string(header))
}

//processes a request
func (conn *connection) request(method, path string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequest(method, conn.url + path, body)
	if err != nil {
		return nil, err
	}
	if conn.username != "" && conn.password != "" {
		addBasicAuthHeaders(conn.username, conn.password, req)
	}
	resp, err := conn.client.Do(req)
	if err != nil {
		return nil, err
	} else if resp.StatusCode >= 400 {
		return resp, parseError(resp)
	} else {
		return resp, nil
	}
}

type Error struct {
	StatusCode int
	URL        string
	Method     string
	ErrorCode  string //empty for HEAD requests
	Reason     string //empty for HEAD requests
}

//stringify the error
func (err *Error) Error() string {
	return fmt.Sprintf("[Error] %v %v: %v %v %v",
		err.Method, err.URL, err.StatusCode, err.ErrorCode, err.Reason)
}

//unmarshalls a JSON Response Body 
func parseBody(resp *http.Response, o interface{}) error {
	err := json.NewDecoder(resp.Body).Decode(&o)
	if err != nil {
		resp.Body.Close()
		return err
	} else {
		return resp.Body.Close()
	}
}

//Parse a CouchDB error response
func parseError(resp *http.Response) error {
	var couchReply struct{ Error, Reason string }
	if resp.Request.Method != "HEAD" {
		err := parseBody(resp, couchReply)
		if err != nil {
			return fmt.Errorf("unknown error accessing CouchDB: %v", err)
		}
	}
	return &Error{
		StatusCode: resp.StatusCode,
		URL:        resp.Request.URL.String(),
		Method:     resp.Request.Method,
		ErrorCode:  couchReply.Error,
		Reason:     couchReply.Reason,
	}
}

//Sanitize path args
func cleanPath(pathSegments ...string) string {
	cleaned := ""
	for _, pathSegment := range pathSegments {
		cleaned += "/"
		cleaned += url.QueryEscape(pathSegment)
	}
	return cleaned
}
