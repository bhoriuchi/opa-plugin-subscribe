package kafka

import (
	"bytes"
	"context"
	"fmt"
	"html/template"
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

var (
	funcMap template.FuncMap = map[string]interface{}{
		"uuid": uuid.NewString,
	}
)

func init() {
	subscribe.SubscribePluginProviders[ProviderName] = NewSubscriber
}

func NewSubscriber(opts *subscribe.NewSubscriberOptions) (subscribe.Subscriber, error) {
	sub := &KafkaSubscriber{}
	logger := opts.Logger.WithFields(map[string]interface{}{
		"provider":   strings.ToUpper(ProviderName),
		"subscriber": opts.Config.GetName(),
		"topic":      opts.Config.Topic,
	})

	logger.Info("Creating new %s subscriber", ProviderName)

	if opts.Config != nil {
		if err := subscribe.Remarshal(opts.Config.Config, sub); err != nil {
			return nil, fmt.Errorf("failed to parse Kafka configuration: %s", err)
		}
	}

	sub.sc = opts.Config
	sub.manager = opts.Manager
	sub.log = logger
	return sub, nil
}

type KafkaSubscriber struct {
	manager      *plugins.Manager
	log          logging.Logger
	sc           *subscribe.SubscriberConfig
	cg           sarama.ConsumerGroup
	c            sarama.Client
	Brokers      []string `yaml:"brokers" json:"brokers"`
	GroupID      string   `yaml:"group_id" json:"group_id"`
	Version      string   `yaml:"version" json:"version"`
	CleanupGroup bool     `yaml:"cleanup_group" json:"cleanup_group"`
}

// connects to nats server
func (s *KafkaSubscriber) Connect(ctx context.Context) error {
	var err error

	if len(s.Brokers) == 0 {
		return fmt.Errorf("no Kafka brokers specified")
	}

	s.log.Info("Connecting to Kafka broker(s) %s", strings.Join(s.Brokers, ", "))

	clusterConfig := sarama.NewConfig()
	if s.Version != "" {
		clusterConfig.Version, err = sarama.ParseKafkaVersion(s.Version)
		if err != nil {
			return fmt.Errorf("invalid Kafka version %s: %s", s.Version, err)
		}
	}

	// the oldest supported version is V0_10_2_0
	if !clusterConfig.Version.IsAtLeast(sarama.V0_10_2_0) {
		if s.Version != "" {
			return fmt.Errorf("version %s was specified but does not support required features", s.Version)
		}

		clusterConfig.Version = sarama.V0_10_2_0
		s.log.Debug("Defaulting to Kafka version %s", clusterConfig.Version)
	}

	clusterConfig.Consumer.Return.Errors = true
	clusterConfig.Consumer.Offsets.Initial = sarama.OffsetNewest
	if s.c, err = sarama.NewClient(s.Brokers, clusterConfig); err != nil {
		s.log.Error("Error creating Kafka client: %s", err)
		return err
	}

	s.log.Info("Successfully connected to Kafka brokers!")
	return nil
}

// subscribe to bundle updates
func (s *KafkaSubscriber) Subscribe(ctx context.Context) error {
	var err error

	if err := s.ensureGroupID(); err != nil {
		s.log.Error(err.Error())
		return err
	}

	// add the group id to the logging fields
	s.log = s.log.WithFields(map[string]interface{}{
		"group_id": s.GroupID,
	})

	s.log.Info("Subscribing to topic")
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
					s.log.Error("Consumer group error: %s", err)
				}
			default:
				err := s.cg.Consume(sctx, topics, h)
				switch err {
				case sarama.ErrClosedConsumerGroup:
					s.log.Debug("Closed consumer group %s", s.GroupID)
					return
				case nil:
					s.log.Debug("Message successfully received!")
					continue
				default:
					s.log.Error("Consume error: %s", err)
				}
			}
		}
	}()

	s.log.Info("Successfully subscribed to topic!")
	return nil
}

// unsubscribe from bundle updates
func (s *KafkaSubscriber) Unsubscribe(ctx context.Context) error {
	s.log.Info("Unsubscribing from topic %s")

	if s.cg == nil {
		s.log.Warn("Attempted to close consumer group, but it does not exist")
		return nil
	}

	if err := s.cg.Close(); err != nil {
		s.log.Error("Failed to close consumer group: %s", err)
		return err
	}

	s.cg = nil
	s.log.Debug("Successfully closed consumer group!")

	if s.CleanupGroup {
		s.log.Debug("Attempting to delete consumer group")

		ca, err := sarama.NewClusterAdminFromClient(s.c)
		if err != nil {
			s.log.Error("Failed to create new Kafka cluster admin client interface: %s", err)
		}

		if err := ca.DeleteConsumerGroup(s.GroupID); err != nil {
			s.log.Error("Failed to delete consumer group: %s", err)
			return err
		}
		s.log.Debug("Successfully deleted consumer group!")
	}

	s.log.Info("Successfully unsubscribed from topic!", s.sc.Topic)
	return nil
}

// disconnect from nats server
func (s *KafkaSubscriber) Disconnect(ctx context.Context) error {
	s.log.Info("Closing client connection")

	if s.c == nil {
		s.log.Warn("Attempted to close client connection, but it does not exist")
		return nil
	}

	if err := s.c.Close(); err != nil {
		s.log.Error("Failed to close client: %s", err)
		return err
	}

	s.c = nil
	s.log.Info("Successfully closed client connection!")
	return nil
}

// gets the group ID. if specified will attempt to interpolate
func (s *KafkaSubscriber) ensureGroupID() error {
	if s.GroupID == "" {
		s.GroupID = uuid.NewString()
	}

	buf := bytes.NewBuffer([]byte{})
	tmpl, err := template.New("groupid").Funcs(funcMap).Parse(s.GroupID)
	if err != nil {
		return fmt.Errorf("failed to parse group_id %s: %s", s.GroupID, err)
	}

	if err := tmpl.Execute(buf, map[string]interface{}{}); err != nil {
		return fmt.Errorf("failed to interpolate group_id %s: %s", s.GroupID, err)
	}

	groupID := buf.Bytes()
	if len(buf.Bytes()) == 0 {
		return fmt.Errorf("group_id %s was interpolated to an empty string", s.GroupID)
	}

	s.GroupID = string(groupID)
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
			h.log.Error("Failed to trigger plugin update: %s", err)
		}
	}
	return nil
}
