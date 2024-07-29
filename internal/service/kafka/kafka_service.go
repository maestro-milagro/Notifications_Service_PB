package kafka_service

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/maestro-milagro/Notifications_Service_PB/internal/lib/sl"
	"github.com/maestro-milagro/Notifications_Service_PB/internal/models"
	"log/slog"
)

type WhoSubber interface {
	WhoSubbed(ctx context.Context, email string) ([]int, error)
}

type KafkaService struct {
	log       *slog.Logger
	whoSubber WhoSubber
}

func New(log *slog.Logger, whoSubber WhoSubber) *KafkaService {
	return &KafkaService{
		log:       log,
		whoSubber: whoSubber,
	}
}

func (kf *KafkaService) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (kf *KafkaService) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (kf *KafkaService) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		var msg models.Post
		err := json.Unmarshal(message.Value, &msg)
		if err != nil {
			fmt.Printf("error while umarshaling: %s\n", err)
		}
		subs, err := kf.whoSubber.WhoSubbed(context.Background(), msg.Email)
		if err != nil {
			kf.log.Error("error while whosubbed check: ", sl.Err(err))
		}
		fmt.Printf("Msg subs: %v\n", subs)
		session.MarkMessage(message, "")
	}

	return nil
}

func (kf *KafkaService) subscribe(ctx context.Context, topic string, consumerGroup sarama.ConsumerGroup) error {
	go func() {
		if err := consumerGroup.Consume(ctx, []string{topic}, kf); err != nil {
			fmt.Printf("error while consuming: %s\n", err)
		}
		if ctx.Err() != nil {
			return
		}
	}()

	return nil
}

var brokers = []string{"localhost:9095"}

func (kf *KafkaService) StartConsuming(ctx context.Context, topic string, brokers []string, groupID string) error {
	config := sarama.NewConfig()

	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	consumerGroup, err := sarama.NewConsumerGroup(brokers, groupID, config)
	if err != nil {
		return err
	}

	return kf.subscribe(ctx, topic, consumerGroup)
}
