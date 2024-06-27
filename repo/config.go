package repo

import (
	"encoding/json"
	"os"
)

type Config struct {
	Lotus     []string `json:"lotus"`
	Providers []string `json:"providers"`
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
	lotus := []string{"eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJBbGxvdyI6WyJyZWFkIl19.4YZ9AyH_nop1chtcj-jwE6TogzCMb1Zo24KIY7jb5nw:/ip4/10.122.6.17/tcp/51234/http"}
	providers := []string{}

	c := &Config{
		Lotus:     lotus,
		Providers: providers,
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
