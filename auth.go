package couchdb

import (
	"net/http"
	"encoding/base64"
)

//Basic interface for Auth
type Auth interface{
	AddAuthHeaders(*http.Request)
}

//HTTP Basic Authentication support
type BasicAuth struct{
	Username string
	Password string
}

//Adds Basic Authentication headers to an http request
func (ba BasicAuth) AddAuthHeaders(req *http.Request){
	authString := []byte(ba.Username + ":" + ba.Password)
	header := "Basic " + base64.StdEncoding.EncodeToString(authString)
	req.Header.Set("Authorization", string(header))
}

//TODO: Add support for other Authentication methods supported by Couch:
//OAuth, Proxy, etc.
