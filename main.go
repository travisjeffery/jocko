package main

import (
	"flag"
	"io/ioutil"
	"log"
	"os"
	"strings"

	"github.com/pkg/errors"
)

var (
	logDirsFlag = flag.String("log-dirs", "/tmp/clog-logs", "A comma separated list of directories under which to store log files")
)

type Clog struct {
	LogDirs []string
}

func NewClog(logDirs []string) (*Clog, error) {
	for _, logDir := range logDirs {
		ld, err := os.Stat(logDir)

		if os.IsNotExist(err) {
			err = os.Mkdir(logDir, 0755)

			if err != nil {
				return nil, errors.Wrap(err, "log directory mkdir failed")
			}
		} else if !ld.IsDir() {
			return nil, errors.Wrap(err, "log directory isn't a directory")
		}
	}

	c := &Clog{
		LogDirs: logDirs,
	}

	return c, nil
}

func (c *Clog) initTopics() (err error) {
	for _, ld := range c.LogDirs {
		fis, err := ioutil.ReadDir(ld)

		if err != nil {
			return errors.Wrap(err, "directory read failed")
		}

		for _, fi := range fis {
			if !fi.IsDir() {
				continue
			}

			if err = c.initTopic(fi.Name()); err != nil {
				break
			}
		}
	}

	return err
}

func (c *Clog) initTopic(name string) error {
	path := filePath
}

func main() {
	flag.Parse()

	_, err := NewClog(strings.Split(*logDirsFlag, ","))

	if err != nil {
		log.Println(err)
	}
}
