package subscribe

import (
	"context"

	"github.com/nats-io/nats.go"
	"github.com/open-policy-agent/opa/plugins"
)

type NatsSubscriber struct {
	conn         *nats.Conn
	sub          *nats.Subscription
	manager      *plugins.Manager
	sc           *SubscriberConfig
	Servers      string `yaml:"servers" json:"servers"`
	MaxReconnect int    `yaml:"max_reconnect" json:"max_reconnect"`
}

// connects to nats server
func (s *NatsSubscriber) Connect(ctx context.Context) error {
	s.manager.Logger().Debug("subscribe: connecting to NATS server(s) %s", s.Servers)
	var err error

	servers := s.Servers
	if servers == "" {
		servers = nats.DefaultURL
	}

	if s.MaxReconnect == 0 {
		s.MaxReconnect = 5
	}

	opts := []nats.Option{
		nats.MaxReconnects(s.MaxReconnect),
	}

	if s.conn, err = nats.Connect(servers, opts...); err != nil {
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
