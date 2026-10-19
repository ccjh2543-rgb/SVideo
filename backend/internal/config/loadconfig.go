package config

import (
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Server              ServerConfig        `yaml:"server"`        // HTTP服务配置
	Database            DatabaseConfig      `yaml:"database"`      // MySQL数据库配置
	Redis               RedisConfig         `yaml:"redis"`         // Redis缓存配置
	RabbitMQ            RabbitMQConfig      `yaml:"rabbitmq"`      // RabbitMQ消息队列配置
	ObservabilityConfig ObservabilityConfig `yaml:"observability"` // 可观测性配置
}

type ServerConfig struct {
	Port int `yaml:"port"` // HTTP服务监听端口
}

type DatabaseConfig struct {
	Host     string `yaml:"host"`     // MySQL主机地址
	Port     int    `yaml:"port"`     // MySQL端口号
	User     string `yaml:"user"`     // MySQL用户名
	Password string `yaml:"password"` // MySQL密码
	DBName   string `yaml:"dbname"`   // MySQL数据库名
}

type RedisConfig struct {
	Host     string `yaml:"host"`     // Redis主机地址
	Port     int    `yaml:"port"`     // Redis端口号
	Password string `yaml:"password"` // Redis密码
	DB       int    `yaml:"db"`       // Redis数据库编号
}

type RabbitMQConfig struct {
	Host     string `yaml:"host"`     // RabbitMQ主机地址
	Port     int    `yaml:"port"`     // RabbitMQ端口号
	Username string `yaml:"username"` // RabbitMQ用户名
	Password string `yaml:"password"` // RabbitMQ密码
}

type ObservabilityConfig struct {
	Pprof PprofConfig `yaml:"pprof"` // pprof性能分析配置
}

type PprofConfig struct {
	Enabled    bool   `yaml:"enabled"`     // 是否启用pprof性能分析
	ApiAddr    string `yaml:"api_addr"`    // API服务pprof监听地址
	WorkerAddr string `yaml:"worker_addr"` // Worker服务pprof监听地址
}

func Load(filename string) (Config, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return Config{}, fmt.Errorf("failed to read config file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return Config{}, fmt.Errorf("parse config %s: %w", filename, err)
	}

	// 环境变量优先级高于配置文件，便于容器化部署和密钥管理。
	applyEnvOverrides(&cfg)

	return cfg, nil
}

// bool用来表示是否使用了默认配置，true表示使用了默认配置
func LoadLocalDev(filename string) (Config, bool, error) {
	cfg, err := Load(filename)
	if err == nil {
		return cfg, false, nil
	}
	// 如果配置文件不存在，使用默认配置
	if errors.Is(err, os.ErrNotExist) {
		defaultCfg := DefaultLocalConfig()
		applyEnvOverrides(&defaultCfg)
		return defaultCfg, true, nil
	}
	return Config{}, false, err
}

// applyEnvOverrides 使用环境变量覆盖配置项。
// 约定：环境变量优先级高于 YAML，空值不覆盖，解析失败时记录日志并保留原值。
func applyEnvOverrides(cfg *Config) {
	if cfg == nil {
		return
	}

	if v := getenv("SERVER_PORT"); v != "" {
		if port, err := strconv.Atoi(v); err == nil {
			cfg.Server.Port = port
		} else {
			log.Printf("invalid SERVER_PORT %q: %v", v, err)
		}
	}

	overrideString(&cfg.Database.Host, "DB_HOST")
	overrideInt(&cfg.Database.Port, "DB_PORT")
	overrideString(&cfg.Database.User, "DB_USER")
	overrideString(&cfg.Database.Password, "DB_PASSWORD")
	overrideString(&cfg.Database.DBName, "DB_NAME")

	overrideString(&cfg.Redis.Host, "REDIS_HOST")
	overrideInt(&cfg.Redis.Port, "REDIS_PORT")
	overrideString(&cfg.Redis.Password, "REDIS_PASSWORD")
	overrideInt(&cfg.Redis.DB, "REDIS_DB")

	overrideString(&cfg.RabbitMQ.Host, "RABBITMQ_HOST")
	overrideInt(&cfg.RabbitMQ.Port, "RABBITMQ_PORT")
	overrideString(&cfg.RabbitMQ.Username, "RABBITMQ_USERNAME")
	overrideString(&cfg.RabbitMQ.Password, "RABBITMQ_PASSWORD")

	if v := getenv("PPROF_ENABLED"); v != "" {
		if enabled, err := strconv.ParseBool(v); err == nil {
			cfg.ObservabilityConfig.Pprof.Enabled = enabled
		} else {
			log.Printf("invalid PPROF_ENABLED %q: %v", v, err)
		}
	}
	overrideString(&cfg.ObservabilityConfig.Pprof.ApiAddr, "PPROF_API_ADDR")
	overrideString(&cfg.ObservabilityConfig.Pprof.WorkerAddr, "PPROF_WORKER_ADDR")
}

// getenv 读取并去除环境变量首尾空白。
func getenv(key string) string {
	return strings.TrimSpace(os.Getenv(key))
}

// overrideString 在环境变量非空时覆盖字符串配置。
func overrideString(target *string, key string) {
	if v := getenv(key); v != "" {
		*target = v
	}
}

// overrideInt 在环境变量非空且合法时覆盖整型配置。
func overrideInt(target *int, key string) {
	v := getenv(key)
	if v == "" {
		return
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		log.Printf("invalid %s %q: %v", key, v, err)
		return
	}
	*target = n
}

// 使用默认配置加载配置文件
func DefaultLocalConfig() Config {
	return Config{
		Server: ServerConfig{
			Port: 8080,
		},
		Database: DatabaseConfig{
			Host:     "localhost",
			Port:     3306,
			User:     "root",
			Password: "",
			DBName:   "feedsystem",
		},
		Redis: RedisConfig{
			Host:     "localhost",
			Port:     6379,
			Password: "",
			DB:       3,
		},
		RabbitMQ: RabbitMQConfig{
			Host:     "localhost",
			Port:     5672,
			Username: "guest",
			Password: "guest",
		},
		ObservabilityConfig: ObservabilityConfig{
			Pprof: PprofConfig{
				Enabled:    true,
				ApiAddr:    "localhost:6060",
				WorkerAddr: "localhost:6061",
			},
		},
	}
}
