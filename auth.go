package couchdb

import (
	"encoding/base64"
	"net/http"
)

//Basic interface for Auth
type Auth interface {
	AddAuthHeaders(*http.Request)
}

//HTTP Basic Authentication support
type BasicAuth struct {
	Username string
	Password string
}

type PassThroughAuth struct {
	AuthHeader string
}

//Adds Basic Authentication headers to an http request
func (ba BasicAuth) AddAuthHeaders(req *http.Request) {
	authString := []byte(ba.Username + ":" + ba.Password)
	header := "Basic " + base64.StdEncoding.EncodeToString(authString)
	req.Header.Set("Authorization", string(header))
}

//Use if you already have an Authentication header you want to pass through to couchdb
func (pta PassThroughAuth) AddAuthHeaders(req *http.Request) {
	req.Header.Set("Authorization", pta.AuthHeader)
}

//TODO: Add support for other Authentication methods supported by Couch:
//OAuth, Proxy, etc.
