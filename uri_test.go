// Copyright (c) 2021 VMware, Inc. or its affiliates. All Rights Reserved.
// Copyright (c) 2012-2021, Sean Treadway, SoundCloud Ltd.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package amqp091

import (
	"reflect"
	"testing"
	"time"
)

// Test matrix defined on http://www.rabbitmq.com/uri-spec.html
type testURI struct {
	url      string
	username string
	password string
	host     string
	port     int
	vhost    string
	canon    string
}

var uriTests = []testURI{
	{
		url:      "amqp://user:pass@host:10000/vhost",
		username: "user",
		password: "pass",
		host:     "host",
		port:     10000,
		vhost:    "vhost",
		canon:    "amqp://user:pass@host:10000/vhost",
	},

	{
		url:      "amqp://",
		username: defaultURI.Username,
		password: defaultURI.Password,
		host:     defaultURI.Host,
		port:     defaultURI.Port,
		vhost:    defaultURI.Vhost,
		canon:    "amqp://localhost/",
	},

	{
		url:      "amqp://:@/",
		username: "",
		password: "",
		host:     defaultURI.Host,
		port:     defaultURI.Port,
		vhost:    defaultURI.Vhost,
		canon:    "amqp://:@localhost/",
	},

	{
		url:      "amqp://user@",
		username: "user",
		password: defaultURI.Password,
		host:     defaultURI.Host,
		port:     defaultURI.Port,
		vhost:    defaultURI.Vhost,
		canon:    "amqp://user@localhost/",
	},

	{
		url:      "amqp://user:pass@",
		username: "user",
		password: "pass",
		host:     defaultURI.Host,
		port:     defaultURI.Port,
		vhost:    defaultURI.Vhost,
		canon:    "amqp://user:pass@localhost/",
	},

	{
		url:      "amqp://guest:pass@",
		username: "guest",
		password: "pass",
		host:     defaultURI.Host,
		port:     defaultURI.Port,
		vhost:    defaultURI.Vhost,
		canon:    "amqp://guest:pass@localhost/",
	},

	{
		url:      "amqp://host",
		username: defaultURI.Username,
		password: defaultURI.Password,
		host:     "host",
		port:     defaultURI.Port,
		vhost:    defaultURI.Vhost,
		canon:    "amqp://host/",
	},

	{
		url:      "amqp://:10000",
		username: defaultURI.Username,
		password: defaultURI.Password,
		host:     defaultURI.Host,
		port:     10000,
		vhost:    defaultURI.Vhost,
		canon:    "amqp://localhost:10000/",
	},

	{
		url:      "amqp:///vhost",
		username: defaultURI.Username,
		password: defaultURI.Password,
		host:     defaultURI.Host,
		port:     defaultURI.Port,
		vhost:    "vhost",
		canon:    "amqp://localhost/vhost",
	},

	{
		url:      "amqp://host/",
		username: defaultURI.Username,
		password: defaultURI.Password,
		host:     "host",
		port:     defaultURI.Port,
		vhost:    defaultURI.Vhost,
		canon:    "amqp://host/",
	},

	{
		url:      "amqp://host/%2F",
		username: defaultURI.Username,
		password: defaultURI.Password,
		host:     "host",
		port:     defaultURI.Port,
		vhost:    "/",
		canon:    "amqp://host/",
	},

	{
		url:      "amqp://host/%2F%2F",
		username: defaultURI.Username,
		password: defaultURI.Password,
		host:     "host",
		port:     defaultURI.Port,
		vhost:    "//",
		canon:    "amqp://host/%2F%2F",
	},

	{
		url:      "amqp://host/%2Fslash%2F",
		username: defaultURI.Username,
		password: defaultURI.Password,
		host:     "host",
		port:     defaultURI.Port,
		vhost:    "/slash/",
		canon:    "amqp://host/%2Fslash%2F",
	},

	{
		url:      "amqp://192.168.1.1:1000/",
		username: defaultURI.Username,
		password: defaultURI.Password,
		host:     "192.168.1.1",
		port:     1000,
		vhost:    defaultURI.Vhost,
		canon:    "amqp://192.168.1.1:1000/",
	},

	{
		url:      "amqp://[::1]",
		username: defaultURI.Username,
		password: defaultURI.Password,
		host:     "::1",
		port:     defaultURI.Port,
		vhost:    defaultURI.Vhost,
		canon:    "amqp://[::1]/",
	},

	{
		url:      "amqp://[::1]:1000",
		username: defaultURI.Username,
		password: defaultURI.Password,
		host:     "::1",
		port:     1000,
		vhost:    defaultURI.Vhost,
		canon:    "amqp://[::1]:1000/",
	},

	{
		url:      "amqp://[fe80::1]",
		username: defaultURI.Username,
		password: defaultURI.Password,
		host:     "fe80::1",
		port:     defaultURI.Port,
		vhost:    defaultURI.Vhost,
		canon:    "amqp://[fe80::1]/",
	},

	{
		url:      "amqp://[fe80::1]",
		username: defaultURI.Username,
		password: defaultURI.Password,
		host:     "fe80::1",
		port:     defaultURI.Port,
		vhost:    defaultURI.Vhost,
		canon:    "amqp://[fe80::1]/",
	},

	{
		url:      "amqp://[fe80::1%25en0]",
		username: defaultURI.Username,
		password: defaultURI.Password,
		host:     "fe80::1%en0",
		port:     defaultURI.Port,
		vhost:    defaultURI.Vhost,
		canon:    "amqp://[fe80::1%25en0]/",
	},

	{
		url:      "amqp://[fe80::1]:5671",
		username: defaultURI.Username,
		password: defaultURI.Password,
		host:     "fe80::1",
		port:     5671,
		vhost:    defaultURI.Vhost,
		canon:    "amqp://[fe80::1]:5671/",
	},

	{
		url:      "amqps:///",
		username: defaultURI.Username,
		password: defaultURI.Password,
		host:     defaultURI.Host,
		port:     schemePorts["amqps"],
		vhost:    defaultURI.Vhost,
		canon:    "amqps://localhost/",
	},

	{
		url:      "amqps://host:1000/",
		username: defaultURI.Username,
		password: defaultURI.Password,
		host:     "host",
		port:     1000,
		vhost:    defaultURI.Vhost,
		canon:    "amqps://host:1000/",
	},
}

func TestURISpec(t *testing.T) {
	for _, test := range uriTests {
		u, err := ParseURI(test.url)
		if err != nil {
			t.Fatal("Could not parse spec URI: ", test.url, " err: ", err)
		}

		if test.username != u.Username {
			t.Error("For: ", test.url, " usernames do not match. want: ", test.username, " got: ", u.Username)
		}

		if test.password != u.Password {
			t.Error("For: ", test.url, " passwords do not match. want: ", test.password, " got: ", u.Password)
		}

		if test.host != u.Host {
			t.Error("For: ", test.url, " hosts do not match. want: ", test.host, " got: ", u.Host)
		}

		if test.port != u.Port {
			t.Error("For: ", test.url, " ports do not match. want: ", test.port, " got: ", u.Port)
		}

		if test.vhost != u.Vhost {
			t.Error("For: ", test.url, " vhosts do not match. want: ", test.vhost, " got: ", u.Vhost)
		}

		if test.canon != u.String() {
			t.Error("For: ", test.url, " canonical string does not match. want: ", test.canon, " got: ", u.String())
		}
	}
}

func TestURIUnknownScheme(t *testing.T) {
	if _, err := ParseURI("http://example.com/"); err == nil {
		t.Fatal("Expected error when parsing non-amqp scheme")
	}
}

func TestURIScheme(t *testing.T) {
	if _, err := ParseURI("amqp://example.com/"); err != nil {
		t.Fatalf("Expected to parse amqp scheme, got %v", err)
	}

	if _, err := ParseURI("amqps://example.com/"); err != nil {
		t.Fatalf("Expected to parse amqps scheme, got %v", err)
	}
}

func TestURIWhitespace(t *testing.T) {
	if _, err := ParseURI("amqp://admin:PASSWORD@rabbitmq-service/ -http_port=8080"); err == nil {
		t.Fatal("Expected to fail if URI contains whitespace")
	}
}

func TestURIDefaults(t *testing.T) {
	url := "amqp://"
	uri, err := ParseURI(url)
	if err != nil {
		t.Fatal("Could not parse")
	}

	if uri.String() != "amqp://localhost/" {
		t.Fatal("Defaults not encoded properly got:", uri.String())
	}
}

func TestURIComplete(t *testing.T) {
	url := "amqp://bob:dobbs@foo.bar:5678/private"
	uri, err := ParseURI(url)
	if err != nil {
		t.Fatal("Could not parse")
	}

	if uri.String() != url {
		t.Fatal("Defaults not encoded properly want:", url, " got:", uri.String())
	}
}

func TestURIDefaultPortAmqpNotIncluded(t *testing.T) {
	url := "amqp://foo.bar:5672/"
	uri, err := ParseURI(url)
	if err != nil {
		t.Fatal("Could not parse")
	}

	if uri.String() != "amqp://foo.bar/" {
		t.Fatal("Defaults not encoded properly got:", uri.String())
	}
}

func TestURIDefaultPortAmqp(t *testing.T) {
	url := "amqp://foo.bar/"
	uri, err := ParseURI(url)
	if err != nil {
		t.Fatal("Could not parse")
	}

	if uri.Port != 5672 {
		t.Fatal("Default port not correct for amqp, got:", uri.Port)
	}
}

func TestURIDefaultPortAmqpsNotIncludedInString(t *testing.T) {
	url := "amqps://foo.bar:5671/"
	uri, err := ParseURI(url)
	if err != nil {
		t.Fatal("Could not parse")
	}

	if uri.String() != "amqps://foo.bar/" {
		t.Fatal("Defaults not encoded properly got:", uri.String())
	}
}

func TestURIDefaultPortAmqps(t *testing.T) {
	url := "amqps://foo.bar/"
	uri, err := ParseURI(url)
	if err != nil {
		t.Fatal("Could not parse")
	}

	if uri.Port != 5671 {
		t.Fatal("Default port not correct for amqps, got:", uri.Port)
	}
}

func TestURITLSConfig(t *testing.T) {
	url := "amqps://foo.bar/?certfile=/foo/%D0%BF%D1%80%D0%B8%D0%B2%D0%B5%D1%82/cert.pem&keyfile=/foo/%E4%BD%A0%E5%A5%BD/key.pem&cacertfile=C:%5Ccerts%5Cca.pem&server_name_indication=example.com"
	uri, err := ParseURI(url)
	if err != nil {
		t.Fatal("Could not parse")
	}
	if uri.CertFile != "/foo/привет/cert.pem" {
		t.Fatal("Certfile not set")
	}
	if uri.CACertFile != "C:\\certs\\ca.pem" {
		t.Fatal("CA not set")
	}
	if uri.KeyFile != "/foo/你好/key.pem" {
		t.Fatal("Key not set")
	}
	if uri.ServerName != "example.com" {
		t.Fatal("Server name not set")
	}
}

func TestURIParameters(t *testing.T) {
	url := "amqps://foo.bar/?auth_mechanism=plain&auth_mechanism=amqpplain&heartbeat=2&connection_timeout=5000&channel_max=8"
	uri, err := ParseURI(url)
	if err != nil {
		t.Fatal("Could not parse")
	}
	if !reflect.DeepEqual(uri.AuthMechanism, []string{"plain", "amqpplain"}) {
		t.Fatal("AuthMechanism not set")
	}
	if !uri.Heartbeat.hasValue {
		t.Fatal("Heartbeat not set")
	}
	if uri.Heartbeat.value != time.Duration(2)*time.Second {
		t.Fatal("Heartbeat not set")
	}
	if uri.ConnectionTimeout != 5000 {
		t.Fatal("ConnectionTimeout not set")
	}
	if uri.ChannelMax != 8 {
		t.Fatal("ChannelMax name not set")
	}
}

func TestURI_ParseUriToString(t *testing.T) {
	tests := []struct {
		name string
		uri  string
		want string
	}{
		{name: "virtual host is set", uri: "amqp://example.com/foobar", want: "amqp://example.com/foobar"},
		{name: "non-default port", uri: "amqp://foo.bar:1234/example", want: "amqp://foo.bar:1234/example"},
		{
			name: "TLS with URI parameters",
			uri:  "amqps://some-host.com/foobar?certfile=/foo/%D0%BF%D1%80%D0%B8%D0%B2%D0%B5%D1%82/cert.pem&keyfile=/foo/%E4%BD%A0%E5%A5%BD/key.pem&cacertfile=C:%5Ccerts%5Cca.pem&server_name_indication=example.com",
			want: "amqps://some-host.com/foobar?certfile=/foo/привет/cert.pem&keyfile=/foo/你好/key.pem&cacertfile=C:\\certs\\ca.pem&server_name_indication=example.com",
		},
		{name: "only server name indication", uri: "amqps://foo.bar?server_name_indication=example.com", want: "amqps://foo.bar/?server_name_indication=example.com"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			amqpUri, err := ParseURI(tt.uri)
			if err != nil {
				t.Errorf("ParseURI() error = %v", err)
			}
			if got := amqpUri.String(); got != tt.want {
				t.Errorf("String() = %v, want %v", got, tt.want)
			}
		})
	}
}
