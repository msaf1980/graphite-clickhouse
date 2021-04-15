package main

import (
	"flag"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"log"
	"strings"

	"go.uber.org/zap"
)

type StringSlice []string

func (u *StringSlice) Set(value string) error {
	*u = append(*u, value)
	return nil
}

func (u *StringSlice) String() string {
	return "[ " + strings.Join(*u, ", ") + " ]"
}

var (
	logger *zap.Logger
	curDir string
)

func main() {
	var err error

	rand.Seed(time.Now().UnixNano())

	curDir, err = filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		log.Fatal(err)
	}

	logger, err = zap.NewProduction()
	if err != nil {
		log.Fatal(err)
	}

	var tests StringSlice
	flag.Var(&tests, "config", "test dir (with test.yml and other)")
	flag.Parse()

	if len(tests) == 0 {
		logger.Fatal("tests config not set")
	}

	var testsFailed []string
	for _, test := range tests {
		logger.Info("Starting test", zap.String("config", test))
		if !runTest(test) {
			testsFailed = append(testsFailed, test)
		}
	}

	if len(testsFailed) > 0 {
		logger.Fatal("FAILED", zap.Strings("tests", testsFailed))
	} else {
		logger.Info("SUCCESS")
	}
}
