package consul

import (
	"fmt"
	"net/http"
	"strings"

	consulapi "github.com/hashicorp/consul/api"
)

type Client struct {
	config *ConsulConfig
	client *consulapi.Client
}

func (c *Client) Consul() *consulapi.Client {
	return c.client
}

func NewClient(conf *ConsulConfig) (*Client, error) {
	var err error

	c := &Client{
		config: conf,
	}

	if c.client, err = conf.Client(); err != nil {
		return nil, err
	}

	return c, nil
}

type ConsulConfig struct {
	WatchType     string `yaml:"watch_type" json:"watch_type" mapstructure:"watch_type"`
	Datacenter    string `yaml:"datacenter" json:"datacenter" mapstructure:"datacenter"`
	Address       string `yaml:"address" json:"address" mapstructure:"address"`
	Scheme        string `yaml:"scheme" json:"scheme" mapstructure:"scheme"`
	HttpAuth      string `yaml:"http_auth" json:"http_auth" mapstructure:"http_auth"`
	Token         string `yaml:"token" json:"token" mapstructure:"token"`
	CAFile        string `yaml:"ca_file" json:"ca_file" mapstructure:"ca_file"`
	CAPem         string `yaml:"ca_pem" json:"ca_pem" mapstructure:"ca_pem"`
	CertFile      string `yaml:"cert_file" json:"cert_file" mapstructure:"cert_file"`
	CertPEM       string `yaml:"cert_pem" json:"cert_pem" mapstructure:"cert_pem"`
	KeyFile       string `yaml:"key_file" json:"key_file" mapstructure:"key_file"`
	KeyPEM        string `yaml:"key_pem" json:"key_pem" mapstructure:"key_pem"`
	CAPath        string `yaml:"ca_path" json:"ca_path" mapstructure:"ca_path"`
	InsecureHttps bool   `yaml:"insecure_https" json:"insecure_https" mapstructure:"insecure_https"`
	Namespace     string `yaml:"namespace" json:"namespace" mapstructure:"namespace"`
}

// Client returns a new client for accessing consul.
func (c *ConsulConfig) Client() (*consulapi.Client, error) {
	config := consulapi.DefaultConfig()
	if c.Datacenter != "" {
		config.Datacenter = c.Datacenter
	}
	if c.Address != "" {
		config.Address = c.Address
	}
	if c.Scheme != "" {
		config.Scheme = c.Scheme
	}

	if c.CAFile != "" {
		config.TLSConfig.CAFile = c.CAFile
	}
	if c.CAPem != "" {
		config.TLSConfig.CAPem = []byte(c.CAPem)
	}
	if c.CertFile != "" {
		config.TLSConfig.CertFile = c.CertFile
	}
	if c.CertPEM != "" {
		config.TLSConfig.CertPEM = []byte(c.CertPEM)
	}
	if c.KeyFile != "" {
		config.TLSConfig.KeyFile = c.KeyFile
	}
	if c.KeyPEM != "" {
		config.TLSConfig.KeyPEM = []byte(c.KeyPEM)
	}
	if c.CAPath != "" {
		config.TLSConfig.CAPath = c.CAPath
	}
	if c.InsecureHttps {
		if config.Scheme != "https" {
			return nil, fmt.Errorf("insecure_https is meant to be used when scheme is https")
		}
		config.TLSConfig.InsecureSkipVerify = c.InsecureHttps
	}

	// This is a temporary workaround to add the Content-Type header when
	// needed until the fix is released in the Consul api client.
	config.HttpClient = &http.Client{
		Transport: transport{config.Transport},
	}

	if config.Transport.TLSClientConfig == nil {
		tlsClientConfig, err := consulapi.SetupTLSConfig(&config.TLSConfig)

		if err != nil {
			return nil, fmt.Errorf("failed to create http client: %s", err)
		}

		config.Transport.TLSClientConfig = tlsClientConfig
	}

	if c.HttpAuth != "" {
		var username, password string
		if strings.Contains(c.HttpAuth, ":") {
			split := strings.SplitN(c.HttpAuth, ":", 2)
			username = split[0]
			password = split[1]
		} else {
			username = c.HttpAuth
		}
		config.HttpAuth = &consulapi.HttpBasicAuth{Username: username, Password: password}
	}

	if c.Token != "" {
		config.Token = c.Token
	}

	client, err := consulapi.NewClient(config)

	if err != nil {
		return nil, err
	}
	return client, nil
}

// transport adds the Content-Type header to all requests that might need it
// until we update the API client to a version with
// https://github.com/hashicorp/consul/pull/10204 at which time we will be able
// to remove this hack.
type transport struct {
	http.RoundTripper
}

func (t transport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Body != nil {
		// This will not be the appropriate Content-Type for the license and
		// snapshot endpoints but this is only temporary and Consul does not
		// actually use the header anyway.
		req.Header.Add("Content-Type", "application/json")
	}
	return t.RoundTripper.RoundTrip(req)
}
