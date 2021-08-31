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
	logger := opts.Logger.WithFields(map[string]interface{}{
		"provider":   strings.ToUpper(ProviderName),
		"subscriber": opts.Config.GetName(),
		"topic":      opts.Config.Topic,
	})

	logger.Info("Creating new %s subscriber", ProviderName)

	if opts.Config != nil {
		if err := subscribe.Remarshal(opts.Config.Config, sub); err != nil {
			return nil, fmt.Errorf("failed to parse configuration: %s", err)
		}
	}

	sub.sc = opts.Config
	sub.manager = opts.Manager
	sub.log = logger
	return sub, nil
}

type NatsSubscriber struct {
	conn         *natspkg.Conn
	sub          *natspkg.Subscription
	manager      *plugins.Manager
	log          logging.Logger
	sc           *subscribe.SubscriberConfig
	Servers      []string `yaml:"servers" json:"servers"`
	MaxReconnect int      `yaml:"max_reconnect" json:"max_reconnect"`
}

// connects to nats server
func (s *NatsSubscriber) Connect(ctx context.Context) error {
	servers := s.Servers
	if len(servers) == 0 {
		servers = []string{natspkg.DefaultURL}
	}

	serverList := strings.Join(servers, ", ")

	s.log.Info("Connecting to server(s) %s", serverList)
	var err error

	if s.MaxReconnect == 0 {
		s.MaxReconnect = 5
	}

	opts := []natspkg.Option{
		natspkg.MaxReconnects(s.MaxReconnect),
	}

	if s.conn, err = natspkg.Connect(serverList, opts...); err != nil {
		s.log.Error("Failed to connect to server(s): %s", err)
		return err
	}

	s.log.Info("Successfully connected to server(s)!")
	return nil
}

// subscribe to bundle updates
func (s *NatsSubscriber) Subscribe(ctx context.Context) error {
	var err error

	s.log.Info("Subscribing to topic")
	s.sub, err = s.conn.Subscribe(s.sc.Topic, func(m *natspkg.Msg) {
		s.log.Debug("Message successfully received!")
		if err := subscribe.Trigger(context.Background(), s.manager, s.sc.Plugin); err != nil {
			s.log.Error("Failed to trigger plugin update: %s", err)
		}
	})

	s.log.Info("Successfully subscribed to topic!")
	return err
}

// unsubscribe from bundle updates
func (s *NatsSubscriber) Unsubscribe(ctx context.Context) error {
	s.log.Info("Unsubscribing from topic")

	if s.sub == nil {
		s.log.Warn("Attempted to unsubscribe from topic, but it does not exist")
		return nil
	}

	if err := s.sub.Unsubscribe(); err != nil {
		s.log.Error("Failed to unsubscribe from topic: %s", err)
		return err
	}

	s.sub = nil
	s.log.Info("Successfully unsubscribed from topic!")
	return nil
}

// disconnect from nats server
func (s *NatsSubscriber) Disconnect(ctx context.Context) error {
	s.log.Info("Draining server connection(s)")

	if s.conn == nil {
		s.log.Warn("Attempted to drain server connection(s), but a connection did not exist")
		return nil
	}

	if err := s.conn.Drain(); err != nil {
		s.log.Error("Failed to drain server connection(s): %s", err)
		return err
	}

	s.conn = nil
	s.log.Info("Successfully drained server connection(s)!")
	return nil
}
