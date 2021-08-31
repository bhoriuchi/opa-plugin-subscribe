package kafka

import (
	"context"
	"fmt"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/bhoriuchi/opa-plugin-subscribe/subscribe"
	"github.com/google/uuid"
	"github.com/open-policy-agent/opa/logging"
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
	sub.log = opts.Logger.WithFields(map[string]interface{}{
		"provider":   strings.ToUpper(ProviderName),
		"subscriber": opts.Config.GetName(),
	})
	return sub, nil
}

type KafkaSubscriber struct {
	manager     *plugins.Manager
	log         logging.Logger
	sc          *subscribe.SubscriberConfig
	cg          sarama.ConsumerGroup
	c           sarama.Client
	Brokers     []string `yaml:"brokers" json:"brokers"`
	GroupID     string   `yaml:"group_id" json:"group_id"`
	DeleteGroup bool     `yaml:"delete_group" json:"delete_group"`
}

// connects to nats server
func (s *KafkaSubscriber) Connect(ctx context.Context) error {
	var err error

	if len(s.Brokers) == 0 {
		return fmt.Errorf("no Kafka brokers specified")
	}

	s.log.Debug("connecting to Kafka brokers %q", s.Brokers)

	clusterConfig := sarama.NewConfig()
	// the oldest supported version is V0_10_2_0
	if !clusterConfig.Version.IsAtLeast(sarama.V0_10_2_0) {
		clusterConfig.Version = sarama.V0_10_2_0
	}
	clusterConfig.Consumer.Return.Errors = true
	clusterConfig.Consumer.Offsets.Initial = sarama.OffsetNewest
	if s.c, err = sarama.NewClient(s.Brokers, clusterConfig); err != nil {
		s.log.Error("error creating Kafka client: %s", err)
		return err
	}

	s.log.Debug("successfully connected Kafka client connection!")
	return nil
}

// subscribe to bundle updates
func (s *KafkaSubscriber) Subscribe(ctx context.Context) error {
	var err error

	if s.GroupID == "" {
		s.GroupID = uuid.New().String()
	}

	s.log.Debug("consumer group %s subscribing to %s", s.GroupID, s.sc.Topic)
	if s.cg, err = sarama.NewConsumerGroupFromClient(s.GroupID, s.c); err != nil {
		return err
	}

	h := &consumerGroupHandler{
		cg:      s.cg,
		manager: s.manager,
		log:     s.log,
		plugin:  s.sc.Plugin,
	}

	sctx := context.Background()
	topics := []string{s.sc.Topic}

	go func() {
		for {
			select {
			case err := <-s.cg.Errors():
				if err != nil {
					s.log.Error("consumer group error: %s", err)
				}
			default:
				err := s.cg.Consume(sctx, topics, h)
				switch err {
				case sarama.ErrClosedConsumerGroup:
					s.log.Debug("closed consumer group %s", s.GroupID)
					return
				case nil:
					continue
				default:
					s.log.Error("consume error: %s", err)
				}
			}
		}
	}()

	return nil
}

// unsubscribe from bundle updates
func (s *KafkaSubscriber) Unsubscribe(ctx context.Context) error {
	s.log.Debug("unsubscribing consumer group %s from topic %s", s.GroupID, s.sc.Topic)

	if s.cg == nil {
		s.log.Warn("attempted to close consumer group %s, but it does not exist", s.GroupID)
		return nil
	}

	if err := s.cg.Close(); err != nil {
		s.log.Error("failed to close consumer group %s", s.GroupID)
		return err
	}

	s.cg = nil
	s.log.Debug("successfully closed consumer group %s", s.GroupID)

	if s.DeleteGroup {
		ca, err := sarama.NewClusterAdminFromClient(s.c)
		if err != nil {
			s.log.Error("failed to create new Kafka cluster admin client interface: %s", err)
		}

		if err := ca.DeleteConsumerGroup(s.GroupID); err != nil {
			s.log.Error("failed to delete consumer group %s: %s", s.GroupID, err)
			return err
		}
	}

	return nil
}

// disconnect from nats server
func (s *KafkaSubscriber) Disconnect(ctx context.Context) error {
	s.log.Debug("closing client connection")

	if s.c == nil {
		s.log.Warn("attempted to close client connection, but it does not exist")
		return nil
	}

	if err := s.c.Close(); err != nil {
		s.log.Error("failed to close client: %s", err)
		return err
	}

	s.c = nil
	return nil
}

// consumer handler
type consumerGroupHandler struct {
	plugin  string
	manager *plugins.Manager
	log     logging.Logger
	cg      sarama.ConsumerGroup
}

func (*consumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (*consumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }
func (h *consumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		if err := subscribe.Trigger(context.Background(), h.manager, h.plugin); err == nil {
			sess.MarkMessage(msg, "")
		} else {
			h.log.Error("failed to trigger plugin update: %s", err)
		}
	}
	return nil
}
