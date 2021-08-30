package subscribe

import (
	"context"
	"fmt"

	"github.com/open-policy-agent/opa/plugins"
	"github.com/segmentio/kafka-go"
)

type KafkaSubscriber struct {
	manager *plugins.Manager
	sc      *SubscriberConfig
	reader  *kafka.Reader
	dialer  *kafka.Dialer
	Brokers []string `yaml:"brokers" json:"brokers"`
	GroupID string   `yaml:"group_id" json:"group_id"`
}

// connects to nats server
func (s *KafkaSubscriber) Connect(ctx context.Context) error {
	// TODO: add more dialer options like auth, ssl, etc.
	s.dialer = kafka.DefaultDialer

	if len(s.Brokers) == 0 {
		return fmt.Errorf("no kafka brokers specified")
	}

	return nil
}

// subscribe to bundle updates
func (s *KafkaSubscriber) Subscribe(ctx context.Context) error {
	s.manager.Logger().Debug("subscriber: subscribing to %s on brokers %q", s.sc.Topic, s.Brokers)
	s.reader = kafka.NewReader(kafka.ReaderConfig{
		Brokers: s.Brokers,
		GroupID: s.GroupID,
		Topic:   s.sc.Topic,
		Dialer:  s.dialer,
	})

	// start reading messages
	go func() {
		for {
			_, err := s.reader.ReadMessage(context.Background())
			if err != nil {
				if err == kafka.ErrGroupClosed {
					s.manager.Logger().Debug("subscriber: closing kafka reader for subscriber %s: %s", s.sc.name, err)
				} else {
					s.manager.Logger().Error("subscriber: failed to read kafka message for subscriber %s: %s", s.sc.name, err)
				}

				break
			}

			// handle the message
			s.manager.Logger().Debug("subscribe: kafka message received on topic %q", s.sc.Topic)
			if err := trigger(context.Background(), s.manager, s.sc.Plugin); err != nil {
				s.manager.Logger().Error(err.Error())
			}
		}
	}()

	return nil
}

// unsubscribe from bundle updates
func (s *KafkaSubscriber) Unsubscribe(ctx context.Context) error {
	if err := s.reader.Close(); err != nil {
		return err
	}

	s.reader = nil
	return nil
}

// disconnect from nats server
func (c *KafkaSubscriber) Disconnect(ctx context.Context) error {
	return nil
}
