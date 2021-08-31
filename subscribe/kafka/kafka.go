package kafka

import (
	"context"
	"fmt"

	"github.com/Shopify/sarama"
	"github.com/bhoriuchi/opa-plugin-subscribe/subscribe"
	"github.com/google/uuid"
	"github.com/open-policy-agent/opa/plugins"
)

const (
	ProviderName = "kafka"
)

func init() {
	subscribe.SubscribePluginProviders[ProviderName] = NewSubscriber
}

func NewSubscriber(opts *subscribe.NewSubscriberOptions) (subscribe.Subscriber, error) {
	sub := &KafkaSubscriber{}

	if opts.Config != nil {
		if err := subscribe.Remarshal(opts.Config.Config, sub); err != nil {
			return nil, fmt.Errorf("failed to parse Kafka configuration for subscriber %s: %s", opts.Config.GetName(), err)
		}
	}

	sub.sc = opts.Config
	sub.manager = opts.Manager
	return sub, nil
}

type KafkaSubscriber struct {
	manager *plugins.Manager
	sc      *subscribe.SubscriberConfig
	cg      sarama.ConsumerGroup
	c       sarama.Client
	Brokers []string `yaml:"brokers" json:"brokers"`
	GroupID string   `yaml:"group_id" json:"group_id"`
}

// connects to nats server
func (s *KafkaSubscriber) Connect(ctx context.Context) error {
	if len(s.Brokers) == 0 {
		return fmt.Errorf("no kafka brokers specified")
	}

	return nil
}

// subscribe to bundle updates
func (s *KafkaSubscriber) Subscribe(ctx context.Context) error {
	var err error

	clusterConfig := sarama.NewConfig()
	// the oldest supported version is V0_10_2_0
	if !clusterConfig.Version.IsAtLeast(sarama.V0_10_2_0) {
		clusterConfig.Version = sarama.V0_10_2_0
	}
	clusterConfig.Consumer.Return.Errors = true
	clusterConfig.Consumer.Offsets.Initial = sarama.OffsetNewest
	if s.c, err = sarama.NewClient(s.Brokers, clusterConfig); err != nil {
		return err
	}

	s.manager.Logger().Debug("subscriber: groups %s subscribing to %s on brokers %q", s.GroupID, s.sc.Topic, s.Brokers)
	if s.cg, err = sarama.NewConsumerGroupFromClient(uuid.New().String(), s.c); err != nil {
		return err
	}

	h := &consumerGroupHandler{
		cg:      s.cg,
		manager: s.manager,
		plugin:  s.sc.Plugin,
	}

	sctx := context.Background()
	topics := []string{s.sc.Topic}
	go func() {
		for {
			select {
			case err := <-s.cg.Errors():
				if err != nil {
					s.manager.Logger().Error(err.Error())
				}
			default:
				err := s.cg.Consume(sctx, topics, h)
				switch err {
				case sarama.ErrClosedConsumerGroup:
					return
				case nil:
					continue
				default:
					s.manager.Logger().Error(err.Error())
				}
			}
		}
	}()

	return nil
}

// unsubscribe from bundle updates
func (s *KafkaSubscriber) Unsubscribe(ctx context.Context) error {
	if s.cg == nil {
		return nil
	}

	return s.cg.Close()
}

// disconnect from nats server
func (s *KafkaSubscriber) Disconnect(ctx context.Context) error {
	if s.c == nil {
		return nil
	}
	return s.c.Close()
}

type consumerGroupHandler struct {
	plugin  string
	manager *plugins.Manager
	cg      sarama.ConsumerGroup
}

func (*consumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (*consumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }
func (h *consumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		if err := subscribe.Trigger(context.Background(), h.manager, h.plugin); err == nil {
			sess.MarkMessage(msg, "")
		} else {
			h.manager.Logger().Error(err.Error())
		}
	}
	return nil
}
