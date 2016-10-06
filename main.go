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
	logDirFlag = flag.String("log-dir", "/tmp/clog-logs", "A comma separated list of directories under which to store log files")
)

type Clog struct {
	LogDir string
}

func NewClog(logDir string) (*Clog, error) {
	ld, err := os.Stat(logDir)

	if os.IsNotExist(err) {
		err = os.Mkdir(logDir, 0755)

		if err != nil {
			return nil, errors.Wrap(err, "log directory mkdir failed")
		}
	} else if !ld.IsDir() {
		return nil, errors.Wrap(err, "log directory isn't a directory")
	}

	c := &Clog{
		LogDir: logDir,
	}

	return c, nil
}

func (c *Clog) initTopics() (err error) {
	fis, err := ioutil.ReadDir(c.LogDir)

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

	return err
}

func (c *Clog) initTopic(name string) error {

}

func main() {
	flag.Parse()

	_, err := NewClog(*logDirFlag)

	if err != nil {
		log.Println(err)
	}
}
