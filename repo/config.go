package repo

import (
	"encoding/json"
	"os"
	"time"
)

type Duration time.Duration

func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	td, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	*d = Duration(td)

	return nil
}

type Config struct {
	Lotus     []string `json:"lotus"`
	Providers []string `json:"providers"`
	Interval  Duration `json:"interval"`
	Parallel  int      `json:"parallel"`
	Limit     int      `json:"limit"`
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
		Interval:  Duration(time.Minute),
		Parallel:  10,
		Limit:     100,
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
