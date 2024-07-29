package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/maestro-milagro/Notifications_Service_PB/internal/config"
	"github.com/maestro-milagro/Notifications_Service_PB/internal/lib/sl"
	"github.com/maestro-milagro/Notifications_Service_PB/internal/service"
	kafka_service "github.com/maestro-milagro/Notifications_Service_PB/internal/service/kafka"
	"github.com/maestro-milagro/Notifications_Service_PB/internal/storage/postgres"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

const (
	envLocal = "local"
	envDev   = "dev"
	envProd  = "prod"
)

type ProductInfo struct {
	SKU   int64
	Price float64
	Cnt   int64
}

type OrderInfo struct {
	UserID    int64
	CreatedAt time.Time
	Products  []ProductInfo
}

type Consumer struct {
}

func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		var msg OrderInfo
		err := json.Unmarshal(message.Value, &msg)
		if err != nil {
			fmt.Printf("error while umarshaling: %s\n", err)
		}
		fmt.Printf("Msg: %d\n", msg.UserID)

		session.MarkMessage(message, "")
	}

	return nil
}

func subscribe(ctx context.Context, topic string, consumerGroup sarama.ConsumerGroup) error {
	consumer := Consumer{}

	go func() {
		if err := consumerGroup.Consume(ctx, []string{topic}, &consumer); err != nil {
			fmt.Printf("error while consuming: %s\n", err)
		}
		if ctx.Err() != nil {
			return
		}
	}()

	return nil
}

func StartConsuming(ctx context.Context, topic string, brokers []string, groupID string) error {
	config := sarama.NewConfig()

	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	consumerGroup, err := sarama.NewConsumerGroup(brokers, groupID, config)
	if err != nil {
		return err
	}

	return subscribe(ctx, topic, consumerGroup)
}

func main() {
	//for {
	//
	//}

	cfg := config.MustLoad()

	log := setupLogger(cfg.Env)

	log.Info(
		"starting user service",
		slog.String("env", cfg.Env),
		slog.String("version", "123"),
	)
	log.Debug("debug messages are enabled")

	dbConf := postgres.Config{
		Host:     cfg.Host,
		Port:     cfg.Port,
		Username: cfg.UserName,
		Password: cfg.Password,
		DBName:   cfg.DBname,
		SSLMode:  cfg.SSLmode,
	}

	storage, err := postgres.New(dbConf)
	if err != nil {
		log.Error("failed to init storage", sl.Err(err))
		os.Exit(1)
	}
	noteService := service.New(log, storage)

	kafkaService := kafka_service.New(log, noteService)

	log.Info("starting server", slog.String("address", cfg.Address))

	done := make(chan os.Signal, 1)
	signal.Notify(done, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	srv := &http.Server{
		Addr:         cfg.Address,
		ReadTimeout:  cfg.HTTPServer.Timeout,
		WriteTimeout: cfg.HTTPServer.Timeout,
		IdleTimeout:  cfg.HTTPServer.IdleTimeout,
	}

	go func() {
		if err := srv.ListenAndServe(); err != nil {
			log.Error("failed to start server")
		}
	}()

	log.Info("server started")

	ctx := context.Background()

	//brokers := []string{"localhost:9095"}

	err = kafkaService.StartConsuming(ctx, "posts", []string{cfg.KafkaBootstrapServer}, "notifications")
	if err != nil {
		return
	}

	<-done
	log.Info("stopping server")

	// TODO: move timeout to config
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Error("failed to stop server", sl.Err(err))

		return
	}

	// TODO: close storage

	log.Info("server stopped")
}

func setupLogger(env string) *slog.Logger {
	var log *slog.Logger

	switch env {
	case envLocal:
		log = slog.New(
			slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}),
		)
	case envDev:
		log = slog.New(
			slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}),
		)
	case envProd:
		log = slog.New(
			slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}),
		)
	default: // If env config is invalid, set prod settings by default due to security
		log = slog.New(
			slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}),
		)
	}

	return log
}
