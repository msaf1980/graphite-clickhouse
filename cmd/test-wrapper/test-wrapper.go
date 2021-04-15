package main

import (
	"fmt"
	"io/ioutil"
	"path"
	"time"

	"go.uber.org/zap"
	"gopkg.in/yaml.v2"
)

type YDuration time.Duration

func (t *YDuration) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var tm string
	if err := unmarshal(&tm); err != nil {
		return err
	}

	td, err := time.ParseDuration(tm)
	if err != nil {
		return fmt.Errorf("failed to parse '%s' to time.Duration: %v", tm, err)
	}

	*t = YDuration(td)
	return nil
}

func (t *YDuration) Duration() time.Duration {
	return time.Duration(*t)
}

type InputSchema struct {
	CchBin  string    `yaml:"carbon_clickhouse_bin"`
	Cch     string    `yaml:"carbon_clickhouse"`
	Metrics []string  `yaml:"metrics"`
	From    YDuration `yaml:"from"`
	Until   YDuration `yaml:"until"`
	Step    YDuration `yaml:"step"`
}

type TestSchema struct {
	Gch string `yaml:"graphite_clickhouse"`
}

type MainConfig struct {
	Version string       `yaml:"version"`
	Ch      []Clickhouse `yaml:"clickhouse"`
	Input   InputSchema  `yaml:"input"`
	Tests   []TestSchema `yaml:"tests"`
}

func runTest(testDir string) bool {
	succesTest := true
	d, err := ioutil.ReadFile(path.Join(testDir, "test.yml"))
	if err != nil {
		logger.Error("Test failed", zap.String("dir", testDir), zap.Error(err))
		return false
	}

	var cfg MainConfig

	err = yaml.Unmarshal(d, &cfg)
	if err != nil {
		logger.Error("Test failed", zap.String("dir", testDir), zap.Error(err))
		return false
	}

	fmt.Printf("%+v\n", cfg)
	for _, db := range cfg.Ch {
		logger.Info("Starting clickhouse", zap.String("dir", db.Dir), zap.String("version", db.Version))
		if err, out := db.Start(); err != nil {
			logger.Error("Failed to start",
				zap.String("dir", db.Dir),
				zap.String("version", db.Version),
				zap.Error(err),
				zap.String("out", out),
			)
			succesTest = false
		}

		if cCh, err := CarbonCLickhouseStart(cfg.Input.CchBin, cfg.Input.Cch, testDir, db.address); err == nil {
			if err := cCh.Stop(); err != nil {
				logger.Error("Failed to stop",
					zap.String("carbon-clickhouse", cfg.Input.Cch),
					zap.Error(err),
				)
			}
		} else {
			logger.Error("Failed to start",
				zap.String("carbon-clickhouse", cfg.Input.Cch),
				zap.Error(err),
			)
		}

		if err, out := db.Stop(true); err != nil {
			logger.Fatal("Failed to stop",
				zap.String("dir", db.Dir),
				zap.String("version", db.Version),
				zap.String("container", db.Container()),
				zap.Error(err),
				zap.String("out", out),
			)
		}
	}
	if succesTest {
		logger.Info("SUCESS", zap.String("config", testDir))
	} else {
		logger.Error("FAILED", zap.String("config", testDir))
	}
	return succesTest
}
