package subscribe

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/open-policy-agent/opa/logging"
	"github.com/open-policy-agent/opa/plugins"
	"gopkg.in/yaml.v2"
)

var (
	SubscribePluginProviders = map[string]NewSubscriberFunc{}
	subscribeLogFields       = map[string]interface{}{
		"plugin": "subscribe",
	}
)

const (
	PluginName    = "subscribe"
	opConnect     = "connect"
	opSubscribe   = "subscribe"
	opUnsubscribe = "unsubscribe"
	opDisconnect  = "disconnect"
)

type NewSubscriberOptions struct {
	Config  *SubscriberConfig
	Manager *plugins.Manager
	Logger  logging.Logger
}

type NewSubscriberFunc func(opts *NewSubscriberOptions) (Subscriber, error)
type SubscriberProvider string

type SubscriberConfig struct {
	name     string
	Provider string      `yaml:"provider" json:"provider"`
	Topic    string      `yaml:"topic" json:"topic"`
	Plugin   string      `yaml:"plugin" json:"plugin"`
	Config   interface{} `yaml:"config" json:"config"`
}

func (s *SubscriberConfig) GetName() string {
	return s.name
}

// validate the provider
func (c *SubscriberConfig) validate(name string) error {
	if c.Provider == "" {
		return fmt.Errorf("no subscriber provider specified for subscriber %s", name)
	}
	if c.Topic == "" {
		return fmt.Errorf("no subscriber topic specified for subscriber %s", name)
	}
	if c.Plugin == "" {
		return fmt.Errorf("no subscriber topic specified for subscriber %s", name)
	}

	return nil
}

type Config struct {
	Subscribers map[string]*SubscriberConfig `yaml:"subscribers" json:"subscribers"`
	subscribers map[string]Subscriber
}

type Subscriber interface {
	Connect(ctx context.Context) error
	Subscribe(ctx context.Context) error
	Unsubscribe(ctx context.Context) error
	Disconnect(ctx context.Context) error
}

type Factory struct{}

type Plugin struct {
	manager *plugins.Manager
	config  Config
	mtx     sync.Mutex
	log     logging.Logger
}

// Start
func (p *Plugin) Start(ctx context.Context) error {
	p.log = p.manager.Logger().WithFields(subscribeLogFields)
	p.log.Debug("starting plugin")

	p.manager.UpdatePluginStatus(PluginName, &plugins.Status{State: plugins.StateOK})
	p.config.subscribers = map[string]Subscriber{}

	for name, s := range p.config.Subscribers {
		s.name = name
		err := s.validate(name)
		if err != nil {
			return err
		}

		newFunc, ok := SubscribePluginProviders[s.Provider]
		if !ok {
			return fmt.Errorf("subscribe provider type %s not registered", s.Provider)
		}

		if p.config.subscribers[name], err = newFunc(&NewSubscriberOptions{
			Config:  s,
			Manager: p.manager,
			Logger:  p.log,
		}); err != nil {
			return err
		}
	}

	// connect and subscribe
	if err := p.op(ctx, opConnect); err != nil {
		return err
	}
	if err := p.op(ctx, opSubscribe); err != nil {
		return err
	}

	return nil
}

// Stop
func (p *Plugin) Stop(ctx context.Context) {
	p.log.Debug("stopping plugin")
	p.manager.UpdatePluginStatus(PluginName, &plugins.Status{State: plugins.StateNotReady})

	// unsubscribe and disconnect
	if err := p.op(ctx, opUnsubscribe); err != nil {
		p.log.Error("failed to unsubscribe: %s", err)
	}

	if err := p.op(ctx, opDisconnect); err != nil {
		p.log.Error("failed to disconnect: %s", err)
	}
}

// Reconfigure
func (p *Plugin) Reconfigure(ctx context.Context, config interface{}) {
	p.log.Debug("reconfiguring plugin")
	p.mtx.Lock()
	defer p.mtx.Unlock()
	p.config = config.(Config)
}

// perform an operation on all subscribers
func (p *Plugin) op(ctx context.Context, name string) error {
	for _, s := range p.config.subscribers {
		switch name {
		case opConnect:
			if err := s.Connect(ctx); err != nil {
				return err
			}
		case opSubscribe:
			if err := s.Subscribe(ctx); err != nil {
				return err
			}
		case opUnsubscribe:
			// should not stop any other unsubscribe ops
			s.Unsubscribe(ctx)
		case opDisconnect:
			// should not stop any other disconnect ops
			s.Disconnect(ctx)
		}
	}

	return nil
}

// New
func (Factory) New(m *plugins.Manager, config interface{}) plugins.Plugin {
	m.UpdatePluginStatus(PluginName, &plugins.Status{State: plugins.StateNotReady})

	return &Plugin{
		manager: m,
		config:  config.(Config),
	}
}

// Validate
func (Factory) Validate(_ *plugins.Manager, config []byte) (interface{}, error) {
	parsedConfig := Config{}

	if err := json.Unmarshal(config, &parsedConfig); err == nil {
		return parsedConfig, nil
	}

	return parsedConfig, yaml.Unmarshal(config, &parsedConfig)
}

func contains(list []string, val string) bool {
	for _, item := range list {
		if item == val {
			return true
		}
	}
	return false
}

// triggers the named plugin
func Trigger(ctx context.Context, manager *plugins.Manager, name string) error {
	if !contains(manager.Plugins(), name) {
		return fmt.Errorf("plugin %q not found", name)
	}

	p := manager.Plugin(name)
	tr, ok := p.(plugins.Triggerable)
	if !ok {
		return fmt.Errorf("plugin %q is not triggerable", name)
	}

	return tr.Trigger(ctx)
}

// hacky way to map one interface to another
func Remarshal(in, out interface{}) error {
	b, err := json.Marshal(in)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, out)
}
