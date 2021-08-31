package nats

import (
	"context"
	"fmt"
	"strings"

	"github.com/bhoriuchi/opa-plugin-subscribe/subscribe"
	natspkg "github.com/nats-io/nats.go"
	"github.com/open-policy-agent/opa/logging"
	"github.com/open-policy-agent/opa/plugins"
)

const (
	ProviderName = "nats"
)

func init() {
	subscribe.SubscribePluginProviders[ProviderName] = NewSubscriber
}

// create a new subscriber
func NewSubscriber(opts *subscribe.NewSubscriberOptions) (subscribe.Subscriber, error) {
	sub := &NatsSubscriber{}

	if opts.Config != nil {
		if err := subscribe.Remarshal(opts.Config.Config, sub); err != nil {
			return nil, fmt.Errorf("failed to parse NATS configuration for subscriber %s: %s", opts.Config.GetName(), err)
		}
	}

	sub.sc = opts.Config
	sub.manager = opts.Manager
	sub.log = opts.Logger.WithFields(map[string]interface{}{
		"provider":   strings.ToUpper(ProviderName),
		"subscriber": opts.Config.GetName(),
	})
	return sub, nil
}

type NatsSubscriber struct {
	conn         *natspkg.Conn
	sub          *natspkg.Subscription
	manager      *plugins.Manager
	log          logging.Logger
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

	s.log.Debug("connecting to NATS server(s) %s", servers)
	var err error

	if s.MaxReconnect == 0 {
		s.MaxReconnect = 5
	}

	opts := []natspkg.Option{
		natspkg.MaxReconnects(s.MaxReconnect),
	}

	if s.conn, err = natspkg.Connect(servers, opts...); err != nil {
		s.log.Error("failed to connect to NATS servers: %s", err)
		return err
	}

	s.log.Debug("connected to NATS servers!")
	return nil
}

// subscribe to bundle updates
func (s *NatsSubscriber) Subscribe(ctx context.Context) error {
	var err error

	s.log.Debug("subscribing to NATS topic %s", s.sc.Topic)
	s.sub, err = s.conn.Subscribe(s.sc.Topic, func(m *natspkg.Msg) {
		s.log.Debug("nats message received on topic %q", s.sc.Topic)
		if err := subscribe.Trigger(context.Background(), s.manager, s.sc.Plugin); err != nil {
			s.log.Error("failed to trigger plugin update: %s", err)
		}
	})

	return err
}

// unsubscribe from bundle updates
func (s *NatsSubscriber) Unsubscribe(ctx context.Context) error {
	s.log.Debug("unsubscribing from topic %s", s.sc.Topic)

	if s.sub == nil {
		s.log.Warn("attempted to unsubscribe from topic %s, but it does not exist", s.sc.Topic)
		return nil
	}

	if err := s.sub.Unsubscribe(); err != nil {
		s.log.Error("failed to unsubscribe from topic %s: %s", s.sc.Topic, err)
		return err
	}

	s.sub = nil
	return nil
}

// disconnect from nats server
func (s *NatsSubscriber) Disconnect(ctx context.Context) error {
	s.log.Debug("draining NATS server connections")

	if s.conn == nil {
		s.log.Warn("attempted to drain server connections, but a connection did not exist")
		return nil
	}

	if err := s.conn.Drain(); err != nil {
		s.log.Error("failed to drain NATS server connections: %s", err)
		return err
	}

	s.conn = nil
	return nil
}
