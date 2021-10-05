package nats

import (
	"context"
	"fmt"
	"strings"

	"github.com/bhoriuchi/opa-plugin-subscribe/subscribe"
	natspkg "github.com/nats-io/nats.go"
	"github.com/oleiade/lane"
	"github.com/open-policy-agent/opa/logging"
	"github.com/open-policy-agent/opa/plugins"
)

const (
	ProviderName = "nats"
)

// register the provider
func init() {
	subscribe.SubscribePluginProviders[ProviderName] = NewSubscriber
}

// Subscriber implements the subscriber interface for nats
type Subscriber struct {
	conn    *natspkg.Conn
	sub     *natspkg.Subscription
	manager *plugins.Manager
	log     logging.Logger
	sc      *subscribe.SubscriberConfig
	dq      *lane.Deque
	config  *Config
}

// Config provides configuration information to nats client
type Config struct {
	Servers      []string `yaml:"servers" json:"servers"`
	MaxReconnect int      `yaml:"max_reconnect" json:"max_reconnect"`
}

// NewSubscriber create a new subscriber
func NewSubscriber(opts *subscribe.NewSubscriberOptions) (subscribe.Subscriber, error) {
	s := &Subscriber{
		sc:      opts.Config,
		manager: opts.Manager,
		config:  &Config{},
		log: opts.Logger.WithFields(map[string]interface{}{
			"provider":   strings.ToUpper(ProviderName),
			"subscriber": opts.Config.GetName(),
			"topic":      opts.Config.Topic,
		}),
	}

	s.log.Info("Creating new %s subscriber", ProviderName)

	if opts.Config == nil {
		return nil, fmt.Errorf("no configuration was specified")
	}

	if err := subscribe.Remarshal(opts.Config.Config, s.config); err != nil {
		return nil, fmt.Errorf("failed to parse configuration: %s", err)
	}

	return s, nil
}

// Connect connects to nats server
func (s *Subscriber) Connect(ctx context.Context) error {
	servers := s.config.Servers
	if len(servers) == 0 {
		servers = []string{natspkg.DefaultURL}
	}

	serverList := strings.Join(servers, ", ")

	s.log.Info("Connecting to server(s) %s", serverList)
	var err error

	if s.config.MaxReconnect == 0 {
		s.config.MaxReconnect = 5
	}

	opts := []natspkg.Option{
		natspkg.MaxReconnects(s.config.MaxReconnect),
	}

	if s.conn, err = natspkg.Connect(serverList, opts...); err != nil {
		s.log.Error("Failed to connect to server(s): %s", err)
		return err
	}

	s.log.Info("Successfully connected to server(s)!")
	return nil
}

// Subscribe subscribes to bundle updates
func (s *Subscriber) Subscribe(ctx context.Context) error {
	var err error

	s.log.Info("Subscribing to topic")
	s.sub, err = s.conn.Subscribe(s.sc.Topic, func(m *natspkg.Msg) {
		s.log.Debug("Message successfully received!")
		if err := subscribe.Trigger(context.Background(), &subscribe.TriggerOptions{
			Name:    s.sc.Plugin,
			Manager: s.manager,
			DQ:      s.dq,
		}); err != nil {
			s.log.Error("Failed to trigger plugin update: %s", err)
		}
	})

	s.log.Info("Successfully subscribed to topic!")
	return err
}

// Unsubscribe unsubscribes from bundle updates
func (s *Subscriber) Unsubscribe(ctx context.Context) error {
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

// Disconnect disconnects from nats server
func (s *Subscriber) Disconnect(ctx context.Context) error {
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
