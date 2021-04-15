package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"text/template"

	"github.com/phayes/freeport"
)

type CarbonCLickhouse struct {
	bin       string
	config    string
	configTpl string
	address   string
}

func CarbonCLickhouseStart(bin, configTpl, testDir, chAddr string) (*CarbonCLickhouse, error) {
	var err error

	if len(bin) == 0 {
		return nil, fmt.Errorf("bin not set")
	}

	c := &CarbonCLickhouse{bin: bin, configTpl: configTpl}
	port, err := freeport.GetFreePort()
	if err != nil {
		return nil, err
	}
	c.address = "127.0.0.1:" + strconv.Itoa(port)

	tmpl, err := template.New(configTpl).ParseFiles(filepath.Join(testDir, configTpl))
	if err != nil {
		return nil, err
	}
	param := struct {
		CH_ADDR  string
		CCH_ADDR string
	}{
		CH_ADDR:  chAddr,
		CCH_ADDR: c.address,
	}
	err = tmpl.Execute(os.Stdout, param)
	if err != nil {
		return nil, err
	}

	return c, nil
}

func (c *CarbonCLickhouse) Stop() error {
	return nil
}
