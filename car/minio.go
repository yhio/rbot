package car

import (
	"encoding/json"
	"os"
)

type MinioConfig struct {
	Endpoint  string `json:"endpoint"`
	AccessKey string `json:"access_key"`
	SecretKey string `json:"secret_key"`
	Bucket    string `json:"bucket"`
	Region    string `json:"region"`
	UseSSL    bool   `json:"use_ssl"`
}

func getMinioConfig() (*MinioConfig, error) {
	minio := os.Getenv("MINIO")
	if minio == "" {
		return nil, nil
	}

	var mc MinioConfig
	conf, err := os.Open(minio)
	if err != nil {
		return nil, err
	}
	defer conf.Close()

	err = json.NewDecoder(conf).Decode(&mc)
	if err != nil {
		return nil, err
	}

	return &mc, nil
}
