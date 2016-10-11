package strolla

import (
	"encoding/json"
	"flag"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/travisjeffery/strolla/commitlog"
)

var (
	logDirFlag = flag.String("logdir", "/tmp/strolla", "A comma separated list of directories under which to store log files")
)

const configFile = "config.json"

type Strolla struct {
	LogDir string
}

func NewStrolla(logDir string) (*Strolla, error) {
	ld, err := os.Stat(logDir)

	if os.IsNotExist(err) {
		err = os.Mkdir(logDir, 0755)

		if err != nil {
			return nil, errors.Wrap(err, "log directory mkdir failed")
		}
	} else if !ld.IsDir() {
		return nil, errors.Wrap(err, "log directory isn't a directory")
	}

	c := &Strolla{
		LogDir: logDir,
	}

	return c, nil
}

func (c *Strolla) initTopics() (err error) {
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

type TopicConfig struct {
}

func (c *Strolla) initTopic(name string) error {
	topicPath := filepath.Join(c.LogDir, name)
	configPath := filepath.Join(topicPath, configFile)

	f, err := os.OpenFile(configPath, os.O_RDWR, 0666)
	if err != nil {
		return errors.Wrap(err, "file open failed")
	}

	d := json.NewDecoder(f)
	var config TopicConfig
	err = d.Decode(&config)
	if err != nil {
		return errors.Wrap(err, "json decode failed")
	}

	t := newTopic(config)
	err = c.register(name, t)

	return err
}

type Topic struct {
	name   string
	config TopicConfig
	log    *commitlog.CommitLog
	writer io.Writer
}

func newTopic(config TopicConfig) *Topic {
	return &Topic{}
}

func (c *Strolla) register(name string, topic *Topic) error {

}
