package main

import (
	"fmt"
	"math"
	"math/rand"
	"os/exec"
	"strconv"

	"github.com/phayes/freeport"
)

type Clickhouse struct {
	Dir     string `yaml:"dir"`
	Version string `yaml:"version"`

	address   string `yaml:"-"`
	container string `yaml:"-"`
}

func (c *Clickhouse) Start() (error, string) {
	if len(c.Version) == 0 {
		return fmt.Errorf("version not set"), ""
	}
	port, err := freeport.GetFreePort()
	if err != nil {
		return err, ""
	}

	c.address = "127.0.0.1:" + strconv.Itoa(port)
	c.container = "gch-clickhouse-" + strconv.FormatInt(rand.Int63n(math.MaxInt64-1), 16)

	chStart := []string{"run", "-d",
		"--name", c.container,
		"--ulimit", "nofile=262144:262144",
		"-p", c.address + ":8123",
		"-v", curDir + "/tests/" + c.Dir + "/config.xml:/etc/clickhouse-server/config.xml",
		"-v", curDir + "/tests/" + c.Dir + "/users.xml:/etc/clickhouse-server/users.xml",
		"-v", curDir + "/tests/" + c.Dir + "/rollup.xml:/etc/clickhouse-server/config.d/rollup.xml",
		"-v", curDir + "/tests/" + c.Dir + "/init.sql:/docker-entrypoint-initdb.d/init.sql",
		"yandex/clickhouse-server:" + c.Version,
	}
	//fmt.Printf("%v\n", chStart)

	cmd := exec.Command("docker", chStart...)
	stdoutStderr, err := cmd.CombinedOutput()

	return err, string(stdoutStderr)
}

func (c *Clickhouse) Stop(delete bool) (error, string) {
	if len(c.container) == 0 {
		return nil, ""
	}

	chStop := []string{"stop", c.container}

	cmd := exec.Command("docker", chStop...)
	stdoutStderr, err := cmd.CombinedOutput()

	if err == nil && delete {
		return c.Delete()
	}
	return err, string(stdoutStderr)
}

func (c *Clickhouse) Delete() (error, string) {
	if len(c.container) == 0 {
		return nil, ""
	}

	chDel := []string{"rm", c.container}

	cmd := exec.Command("docker", chDel...)
	stdoutStderr, err := cmd.CombinedOutput()

	return err, string(stdoutStderr)
}

func (c *Clickhouse) Address() string {
	return c.address
}

func (c *Clickhouse) Container() string {
	return c.container
}
