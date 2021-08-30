package subscribe

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/open-policy-agent/opa/plugins"
	"gopkg.in/yaml.v2"
)

const (
	PluginName    = "subscribe"
	opConnect     = "connect"
	opSubscribe   = "subscribe"
	opUnsubscribe = "unsubscribe"
	opDisconnect  = "disconnect"
)

type Config struct {
	Nats        *NatsConfig `yaml:"nats" json:"nats"`
	subscribers []Subscriber
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
}

// Start
func (p *Plugin) Start(ctx context.Context) error {
	p.manager.Logger().Debug("subscribe: starting plugin")
	p.manager.UpdatePluginStatus(PluginName, &plugins.Status{State: plugins.StateOK})
	p.config.subscribers = []Subscriber{}

	// set up subscribers
	if p.config.Nats != nil {
		p.config.Nats.manager = p.manager
		p.config.subscribers = append(p.config.subscribers, p.config.Nats)
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
	p.manager.Logger().Debug("subscribe: stopping plugin")
	p.manager.UpdatePluginStatus(PluginName, &plugins.Status{State: plugins.StateNotReady})

	// unsubscribe and disconnect
	if err := p.op(ctx, opUnsubscribe); err != nil {
		p.manager.Logger().Error("subscribe: failed to unsubscribe: %s", err)
	}

	if err := p.op(ctx, opDisconnect); err != nil {
		p.manager.Logger().Error("subscribe: failed to disconnect: %s", err)
	}
}

// Reconfigure
func (p *Plugin) Reconfigure(ctx context.Context, config interface{}) {
	p.manager.Logger().Debug("subscribe: reconfiguring plugin")
	p.mtx.Lock()
	defer p.mtx.Unlock()
	p.config = config.(Config)
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
func trigger(ctx context.Context, manager *plugins.Manager, name string) error {
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

// perform an operation on all subscribers
func (p *Plugin) op(ctx context.Context, name string) error {
	for _, s := range p.config.subscribers {
		switch name {
		case opConnect:
			return s.Connect(ctx)
		case opSubscribe:
			return s.Subscribe(ctx)
		case opUnsubscribe:
			return s.Unsubscribe(ctx)
		case opDisconnect:
			return s.Disconnect(ctx)
		}
	}

	return nil
}
