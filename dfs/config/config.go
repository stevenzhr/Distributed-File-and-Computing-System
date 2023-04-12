package config

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
)

const (
	TIME_FORMAT          = "2006-01-02 15:04:05"
	VAULT_PATH           = "/bigdata/students/hzhang129/vault/"
	REPORT_GAP_THRESHOLD = 21
	HEART_BEAT_RATE      = 3
	CHUNK_SIZE           = 64 * 1024 * 1024  // 64 MB
	RESERVED_SPACE       = 256 * 1024 * 1024 // 256 MB
)

type Config struct {
	VaultPath          string `json:"VaultPath"`
	ReportGapThreshold int    `json:"ReportGapThreshold"`
	HeartBeatRate      int    `json:"HeartBeatRate"`
	Host               string `json:"Host"`
	Port               string `json:"Port"`
	NodeName           string `json:"NodeName"`
	// ChunkSize          int    `json:"ChunkSize"`
}

// Save config struct to a local JSON file
func (config *Config) SaveConfig(filename string) bool {
	file, err := os.Create(filename)
	if err != nil {
		log.Printf("WARNING: Can not create config JSON file. %s \n", err.Error())
		return false
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	err = encoder.Encode(config)
	if err != nil {
		log.Printf("WARNING: Faile to save config parameter. %s", err)
		return false
	}
	return true
}

// Load config struct from a local JSON file
func (config *Config) LoadConfig(filename string) bool {
	file, err := os.Open(filename)
	if err != nil {
		log.Printf("WARNING: Can not open config JSON file. Will load default config. %s \n", err.Error())
		setDefaultConfig(config)
		return false
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	err = decoder.Decode(&config)
	if err != nil {
		log.Printf("WARNING: Faile to load config parameter. Use default instead. %s", err)
		setDefaultConfig(config)
		return false
	}
	return true
}

func (config *Config) String() string {
	var resStr string
	resStr += fmt.Sprintf("VaultPath: %s \n", config.VaultPath)
	resStr += fmt.Sprintf("ReportGapThreshold: %d \n", config.ReportGapThreshold)
	resStr += fmt.Sprintf("HeartBeatRate: %d \n", config.HeartBeatRate)
	resStr += fmt.Sprintf("Host: %s \n", config.Host)
	resStr += fmt.Sprintf("Port: %s \n", config.Port)
	resStr += fmt.Sprintf("NodeName: %s \n", config.NodeName)
	return resStr
}

// When fail to load config struct from JSON file,
// use this method to set default value.
func setDefaultConfig(config *Config) {
	config.VaultPath = VAULT_PATH
	config.ReportGapThreshold = REPORT_GAP_THRESHOLD
	config.HeartBeatRate = HEART_BEAT_RATE
	// config.ChunkSize = CHUNK_SIZE
}
