package nats

import (
	"context"
	"fmt"

	"github.com/bhoriuchi/opa-plugin-subscribe/subscribe"
	natspkg "github.com/nats-io/nats.go"
	"github.com/open-policy-agent/opa/plugins"
)

const (
	ProviderName = "nats"
)

func init() {
	subscribe.SubscribePluginProviders[ProviderName] = NewSubscriber
}

func NewSubscriber(opts *subscribe.NewSubscriberOptions) (subscribe.Subscriber, error) {
	sub := &NatsSubscriber{}

	if opts.Config != nil {
		if err := subscribe.Remarshal(opts.Config.Config, sub); err != nil {
			return nil, fmt.Errorf("failed to parse NATS configuration for subscriber %s: %s", opts.Config.GetName(), err)
		}
	}

	sub.sc = opts.Config
	sub.manager = opts.Manager
	return sub, nil
}

type NatsSubscriber struct {
	conn         *natspkg.Conn
	sub          *natspkg.Subscription
	manager      *plugins.Manager
	sc           *subscribe.SubscriberConfig
	Servers      string `yaml:"servers" json:"servers"`
	MaxReconnect int    `yaml:"max_reconnect" json:"max_reconnect"`
}

// connects to nats server
func (s *NatsSubscriber) Connect(ctx context.Context) error {
	servers := s.Servers
	if servers == "" {
		servers = natspkg.DefaultURL
	}

	s.manager.Logger().Debug("subscribe: connecting to NATS server(s) %s", servers)
	var err error

	if s.MaxReconnect == 0 {
		s.MaxReconnect = 5
	}

	opts := []natspkg.Option{
		natspkg.MaxReconnects(s.MaxReconnect),
	}

	if s.conn, err = natspkg.Connect(servers, opts...); err != nil {
		return err
	}

	s.manager.Logger().Debug("subscribe: connected to NATS servers!")
	return nil
}

// subscribe to bundle updates
func (s *NatsSubscriber) Subscribe(ctx context.Context) error {
	var err error

	s.manager.Logger().Debug("subscribe: subscribing to NATS topic %s", s.sc.Topic)
	s.sub, err = s.conn.Subscribe(s.sc.Topic, func(m *natspkg.Msg) {
		s.manager.Logger().Debug("subscribe: nats message received on topic %q", s.sc.Topic)
		if err := subscribe.Trigger(context.Background(), s.manager, s.sc.Plugin); err != nil {
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

	s.manager.Logger().Debug("subscribe: unsubscribing from NATS topic %s", s.sc.Topic)
	if err := s.sub.Unsubscribe(); err != nil {
		return err
	}

	s.sub = nil
	return nil
}

// disconnect from nats server
func (s *NatsSubscriber) Disconnect(ctx context.Context) error {
	if s.conn == nil {
		return nil
	}
	s.manager.Logger().Debug("subscribe: draining NATS server connections")
	return s.conn.Drain()
}
