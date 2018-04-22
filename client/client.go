package client
import (
	"sync"
	"errors"
	"time"
	"fmt"
	"strings"
	"strconv"

	"github.com/travisjeffery/jocko/protocol"
)

//simple client
type Client struct {
	sync.Mutex
	conn *Conn //conn to any non-controller broker
	bootstrapBrokers []string
	connBroker string
	config *ClientConfig
}

type ClientConfig struct {
	//net conn
	DialTimeout time.Duration
	ReadTimeout time.Duration
	WriteTimeout time.Duration
	//metadata
	MaxRetry int
	RetryBackoff time.Duration
	RefreshFrequency time.Duration
}

func NewClient(bootBrokers []string, cfg *ClientConfig) (cli *Client, err error) {
	cli = &Client{bootstrapBrokers:bootBrokers,config:cfg}
	if len(bootBrokers)>0 {
		for _, broker := range bootBrokers {
			cli.conn, err = Dial("tcp",broker)
			if err == nil {
				cli.connBroker = broker
				break
			}
		}
		if err != nil { //failed to conn to any broker
			return nil, errors.New(fmt.Sprintf("fail to connect to any of %v\n",bootBrokers))
		}
	}
	return cli, nil
}

func (cli *Client) Close() {
	cli.Lock()
	defer cli.Unlock()
	if cli.conn != nil {
		cli.conn.Close()
	}
}

type NodeTopicInfo struct {
	TopicName string
	PartitionID int32
	IsInternal bool
	IsLeader bool
	Replicas []int32
	Isr []int32
}

func (cli *Client) DescribeNodes(nodes []string, opt *Options) (info map[string][]*NodeTopicInfo, err error) {
	cli.Lock()
	defer cli.Unlock()
	resp, err := cli.conn.Metadata(&protocol.MetadataRequest{
		APIVersion: 1,
		Topics: nil,
		AllowAutoTopicCreation: true,
	})
	if err != nil {
		return nil, err
	}
	info = make(map[string][]*NodeTopicInfo)
	for _, nname := range nodes {
		nid := int32(-1)
		nameport := strings.Split(nname,":")
		host := nameport[0]
		port, err := strconv.Atoi(nameport[1])
		if err!=nil { continue }
		for _, b := range resp.Brokers {
			if host == b.Host && port==int(b.Port) {
				nid = b.NodeID
				break
			}
		}
		if nid < 0 {
			fmt.Println("failed to find node:",nname)
			continue
		}
		var nti []*NodeTopicInfo
		for _,tm := range resp.TopicMetadata {
			for _,part := range tm.PartitionMetadata {
				for _,rep := range part.Replicas {
					if nid == rep {
						nti = append(nti,&NodeTopicInfo{TopicName:tm.Topic,PartitionID:part.ParititionID,IsInternal:tm.IsInternal,IsLeader:(nid==part.Leader),Replicas:part.Replicas,Isr:part.ISR})
						break
					}
				}
			}
		}
		info[nname]=nti
	}
	return info, nil
}
