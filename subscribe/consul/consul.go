package consul

import (
	"context"
	"fmt"
	"strings"

	"github.com/bhoriuchi/opa-plugin-subscribe/subscribe"
	"github.com/hashicorp/consul/api/watch"
	"github.com/oleiade/lane"
	"github.com/open-policy-agent/opa/logging"
	"github.com/open-policy-agent/opa/plugins"
)

const (
	ProviderName = "consul"
)

// register provider
func init() {
	subscribe.SubscribePluginProviders[ProviderName] = NewSubscriber
}

// Subscriber implements the subscriber interface for consul
type Subscriber struct {
	manager *plugins.Manager
	log     logging.Logger
	sc      *subscribe.SubscriberConfig
	config  *ConsulConfig
	client  *Client
	wp      *watch.Plan
	dq      *lane.Deque
}

// create a new subscriber
func NewSubscriber(opts *subscribe.NewSubscriberOptions) (subscribe.Subscriber, error) {
	s := &Subscriber{
		sc:      opts.Config,
		manager: opts.Manager,
		config:  &ConsulConfig{},
		dq:      lane.NewCappedDeque(1),
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
func (s *Subscriber) Connect(ctx context.Context) (err error) {
	s.log.Debug("Connecting to consul at %s", s.config.Address)
	if s.client != nil {
		err = fmt.Errorf("already connected")
		return
	}

	if s.client, err = NewClient(s.config); err != nil {
		return
	}

	return
}

// Subscribe to bundle updates
func (s *Subscriber) Subscribe(ctx context.Context) (err error) {
	if s.wp != nil {
		err = fmt.Errorf("watcher already started")
		return
	}

	if s.sc.Topic == "" {
		err = fmt.Errorf("no topic specified")
		return
	}

	params := map[string]interface{}{
		"type": s.config.WatchType,
	}

	// generate params. empty watcher type defaults to event
	switch s.config.WatchType {
	case "key":
		params["key"] = s.sc.Topic
	case "keyprefix":
		params["prefix"] = s.sc.Topic
	case "event", "":
		params["type"] = "event"
		params["name"] = s.sc.Topic
	default:
		err = fmt.Errorf("unsupported watch type %q", s.config.WatchType)
		return
	}

	if s.wp, err = watch.Parse(params); err != nil {
		return
	}

	s.wp.HybridHandler = func(bv watch.BlockingParamVal, data interface{}) {
		s.log.Debug("Received a message")
		if err := subscribe.Trigger(context.Background(), &subscribe.TriggerOptions{
			Name:    s.sc.Plugin,
			Manager: s.manager,
			DQ:      s.dq,
		}); err != nil {
			s.log.Error("Failed to trigger plugin update: %s", err)
		}
	}

	go func() {
		s.log.Debug("Watching topic")
		if err := s.wp.RunWithClientAndHclog(s.client.Consul(), s.wp.Logger); err != nil {
			s.log.Error("watcher failed: %s", err)
		}
	}()

	return
}

// unsubscribe from bundle updates
func (s *Subscriber) Unsubscribe(ctx context.Context) (err error) {
	s.log.Info("Unsubscribing from topic")
	if s.wp == nil || s.wp.IsStopped() {
		err = fmt.Errorf("consul watcher is already stopped")
		return
	}

	s.wp.Stop()
	s.wp = nil
	s.log.Info("Successfully unsubscribed from topic!")
	return nil
}

// Disconnect disconnect from nats server
func (s *Subscriber) Disconnect(ctx context.Context) (err error) {
	s.log.Info("Disconnecting")
	if s.client == nil {
		err = fmt.Errorf("not connected")
		return
	}

	s.Unsubscribe(ctx)
	s.log.Info("Successfully disconnected!")
	return
}
