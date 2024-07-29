package config

import (
	"github.com/ilyakaznacheev/cleanenv"
	"log"
	"os"
	"time"
)

type Config struct {
	Env                  string `yaml:"env" env-default:"local"`
	KafkaBootstrapServer string `yaml:"kafka_bootstrap_server" env-default:"localhost:9095"`
	DB                   `yaml:"db"`
	HTTPServer           `yaml:"http_server"`
}

type HTTPServer struct {
	Address     string        `yaml:"address" env-default:"localhost:8080"`
	Timeout     time.Duration `yaml:"timeout" env-default:"4s"`
	IdleTimeout time.Duration `yaml:"idle_timeout" env-default:"60s"`
}

type DB struct {
	DataBase string `yaml:"database"`
	UserName string `yaml:"username"`
	Password string `yaml:"password"`
	Host     string `yaml:"host"`
	Port     string `yaml:"port"`
	DBname   string `yaml:"dbname"`
	SSLmode  string `yaml:"sslmode"`
}

func MustLoad() *Config {
	configPath := "config/prod.yaml"
	if configPath == "" {
		log.Fatal("CONFIG_PATH is not set")
	}

	// check if file exists
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		log.Fatalf("config file does not exist: %s", configPath)
	}

	var cfg Config

	if err := cleanenv.ReadConfig(configPath, &cfg); err != nil {
		log.Fatalf("cannot read config: %s", err)
	}

	return &cfg
}
