package subscribe

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go"
	"github.com/open-policy-agent/opa/plugins"
)

type NatsSubscriber struct {
	conn         *nats.Conn
	sub          *nats.Subscription
	manager      *plugins.Manager
	sc           *SubscriberConfig
	URL          string `yaml:"url" json:"url"`
	MaxReconnect int    `yaml:"max_reconnect" json:"max_reconnect"`
}

// connects to nats server
func (s *NatsSubscriber) Connect(ctx context.Context) error {
	s.manager.Logger().Debug("subscribe: connecting to NATS server %s", s.URL)
	var err error

	u := s.URL
	if u == "" {
		u = nats.DefaultURL
	}

	if s.MaxReconnect == 0 {
		s.MaxReconnect = 5
	}

	if s.sc.Topic == "" {
		return fmt.Errorf("no topic specified")
	}

	if s.sc.Plugin == "" {
		return fmt.Errorf("no target plugin was specified")
	}

	opts := []nats.Option{
		nats.MaxReconnects(s.MaxReconnect),
	}

	if s.conn, err = nats.Connect(u, opts...); err != nil {
		return err
	}

	return nil
}

// subscribe to bundle updates
func (s *NatsSubscriber) Subscribe(ctx context.Context) error {
	var err error

	s.sub, err = s.conn.Subscribe(s.sc.Topic, func(m *nats.Msg) {
		s.manager.Logger().Debug("subscribe: nats message received on topic %q", s.sc.Topic)
		if err := trigger(context.Background(), s.manager, s.sc.Plugin); err != nil {
			s.manager.Logger().Error(err.Error())
		}
	})

	return err
}

// unsubscribe from bundle updates
func (s *NatsSubscriber) Unsubscribe(ctx context.Context) error {
	if s.sub == nil {
		return nil
	}

	if err := s.sub.Unsubscribe(); err != nil {
		return err
	}

	s.sub = nil
	return nil
}

// disconnect from nats server
func (c *NatsSubscriber) Disconnect(ctx context.Context) error {
	return c.conn.Drain()
}
