package repo

import (
	"encoding/json"
	"os"
)

type Config struct {
	ServerAddr string            `json:"serverAddr"`
	Providers  map[string]string `json:"providers"`
	Parallel   int               `json:"parallel"`
	Limit      int               `json:"limit"`
}

func loadConfig(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var c Config
	err = json.Unmarshal(raw, &c)
	if err != nil {
		return nil, err
	}

	return &c, nil
}

func defaultConfig() *Config {
	providers := map[string]string{}

	c := &Config{
		ServerAddr: "127.0.0.1:9876",
		Providers:  providers,
		Parallel:   10,
		Limit:      100,
	}

	return c
}

func initConfig(path string) error {
	data, err := json.MarshalIndent(defaultConfig(), "", "\t")
	if err != nil {
		return err
	}

	return os.WriteFile(path, data, 0666)
}
