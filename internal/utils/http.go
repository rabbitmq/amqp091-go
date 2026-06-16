// Copyright (c) 2026 Broadcom. All Rights Reserved.
// The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

//go:build integration

package utils

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
)

type ClientProperties struct {
	ConnectionName string `json:"connection_name"`
}

type Connection struct {
	Name             string           `json:"name"`
	ClientProperties ClientProperties `json:"client_properties"`
}

func Connections() ([]Connection, error) {
	bodyString, err := httpGet("http://localhost:15672/api/connections/", "guest", "guest")
	if err != nil {
		return nil, err
	}

	var data []Connection
	err = json.Unmarshal([]byte(bodyString), &data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func GetConnectionByName(name string) (*Connection, error) {
	connections, err := Connections()
	if err != nil {
		return nil, err
	}
	for _, conn := range connections {
		if conn.ClientProperties.ConnectionName == name {
			return &conn, nil
		}
	}

	return nil, errors.New("connection not found")
}

func DropConnection(name string, port string) error {
	_, err := httpDelete("http://localhost:"+port+"/api/connections/"+name, "guest", "guest")
	if err != nil {
		return err
	}

	return nil
}

func httpGet(url, username, password string) (string, error) {
	return baseCall(url, username, password, "GET")
}

func httpDelete(url, username, password string) (string, error) {
	return baseCall(url, username, password, "DELETE")
}

func baseCall(url, username, password string, method string) (string, error) {
	var client http.Client
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		return "", err
	}
	req.SetBasicAuth(username, password)

	resp, err := client.Do(req)

	if err != nil {
		return "", err
	}

	defer resp.Body.Close()

	if resp.StatusCode == 200 { // OK
		bodyBytes, err := io.ReadAll(resp.Body)
		if err != nil {
			return "", err
		}
		return string(bodyBytes), nil
	}

	if resp.StatusCode == 201 {
		// Created! it is ok
		return "", nil
	}

	if resp.StatusCode == 204 { // No Content
		return "", nil
	}

	return "", errors.New(strconv.Itoa(resp.StatusCode))

}
