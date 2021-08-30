package subscribe

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go"
	"github.com/open-policy-agent/opa/plugins"
)

type NatsConfig struct {
	conn         *nats.Conn
	sub          *nats.Subscription
	manager      *plugins.Manager
	URL          string `yaml:"url" json:"url"`
	Topic        string `yaml:"topic" json:"topic"`
	Plugin       string `yaml:"plugin" json:"plugin"`
	MaxReconnect int    `yaml:"max_reconnect" json:"max_reconnect"`
}

// connects to nats server
func (c *NatsConfig) Connect(ctx context.Context) error {
	c.manager.Logger().Debug("subscribe: connecting to NATS server %s", c.URL)
	var err error

	u := c.URL
	if u == "" {
		u = nats.DefaultURL
	}

	if c.MaxReconnect == 0 {
		c.MaxReconnect = 5
	}

	if c.Topic == "" {
		return fmt.Errorf("no topic specified")
	}

	if c.Plugin == "" {
		return fmt.Errorf("no target plugin was specified")
	}

	opts := []nats.Option{
		nats.MaxReconnects(c.MaxReconnect),
	}

	if c.conn, err = nats.Connect(u, opts...); err != nil {
		return err
	}

	return nil
}

// subscribe to bundle updates
func (c *NatsConfig) Subscribe(ctx context.Context) error {
	var err error

	c.sub, err = c.conn.Subscribe(c.Topic, func(m *nats.Msg) {
		c.manager.Logger().Debug("subscribe: nats message received on topic %q", c.Topic)
		if err := trigger(context.Background(), c.manager, c.Plugin); err != nil {
			c.manager.Logger().Error(err.Error())
		}
	})

	return err
}

// unsubscribe from bundle updates
func (c *NatsConfig) Unsubscribe(ctx context.Context) error {
	if c.sub == nil {
		return nil
	}

	if err := c.sub.Unsubscribe(); err != nil {
		return err
	}

	c.sub = nil
	return nil
}

// disconnect from nats server
func (c *NatsConfig) Disconnect(ctx context.Context) error {
	return c.conn.Drain()
}
