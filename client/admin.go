package client

import (
	"time"
	"errors"
	"fmt"

	"github.com/travisjeffery/jocko/protocol"
)

//simple admin client (derived from KIP-117)
type AdminClient struct {
	*Client
	//controller broker of cluster
	ctrlConn *Conn //conn to controller broker
	ctrlBroker string
	//controller *protocol.Broker
}

func NewAdminClient(bootBrokers []string, cfg *ClientConfig) (adminCli *AdminClient, err error) {
	adminCli = &AdminClient{}
	adminCli.Client, err = NewClient(bootBrokers,cfg)
	return adminCli, nil
}

func (adm *AdminClient) connectController() (err error) {
	//fetch metadata and controller broker info
	//for create_topic/delete_topic, need conn to controller
	if adm.conn == nil {
		return errors.New("Admin client is not connected to any broker")
	} else if adm.ctrlConn != nil {
		return
	}
	cinfo, err := adm.DescribeCluster(DefaultOptions)
	if err != nil {
		return err
	}
	adm.ctrlBroker = cinfo.Controller.Addr()

	if adm.connBroker == adm.ctrlBroker {
		adm.ctrlConn = adm.conn
	} else {
		adm.ctrlConn, err = Dial("tcp", adm.ctrlBroker)
		if err!=nil {
			return errors.New(fmt.Sprintf("failed to dial controller broker %s\n",adm.ctrlBroker))
		}
		fmt.Println("connect to controller broker: ",adm.ctrlBroker)
	}
	return nil
}

func (adm *AdminClient) Close() {
	adm.Lock()
	defer adm.Unlock()
	if adm.conn!=nil { adm.conn.Close() }
	if adm.ctrlConn!=nil && adm.ctrlConn!=adm.conn { adm.ctrlConn.Close() }
}

type Options struct {
	Timeout time.Duration
	ValidateOnly bool
}

var (
	DefaultOptions = &Options{10*time.Second,false}
)


type TopicInfo struct {
	Topic             string
	Partitions        int32
	ReplicationFactor int32
}

func (adm *AdminClient) CreateTopics(topics []*TopicInfo, opt *Options) (err error) {
	fmt.Println("create topic")
	// Create/Delete_Topics need conn to controller borker
	//do connectController() outside local lock, since it internally locks too in DescribeLock()
	if err = adm.connectController(); err!=nil {
		return err
	} else {
		fmt.Println("connect to controller broker: ",adm.ctrlBroker)
	}
	adm.Lock()
	defer adm.Unlock()
	var reqs []*protocol.CreateTopicRequest
	for _, ti := range topics {
		reqs = append(reqs, &protocol.CreateTopicRequest{
			Topic:             ti.Topic,
			NumPartitions:     ti.Partitions,
			ReplicationFactor: int16(ti.ReplicationFactor),
			ReplicaAssignment: nil,
			Configs:           nil,
		})
	}
	resp, err := adm.ctrlConn.CreateTopics(&protocol.CreateTopicRequests{
		Requests: reqs,
	})
	if err != nil {
		return err
	}
	for _, topicErrCode := range resp.TopicErrorCodes {
		if topicErrCode.ErrorCode != protocol.ErrNone.Code() &&
			topicErrCode.ErrorCode != protocol.ErrRequestTimedOut.Code() {
			err = protocol.Errs[topicErrCode.ErrorCode]
			return err
		}
	}
	return nil
}

func (adm *AdminClient) DeleteTopics(topics []string, opt *Options) (err error) {
	// Create/Delete_Topics need conn to controller borker
	//do connectController() outside local lock, since it internally locks too in DescribeCluster()
	if err = adm.connectController(); err!=nil {
		return err
	} else {
		fmt.Println("connect to controller broker: ",adm.ctrlBroker)
	}
	adm.Lock()
	defer adm.Unlock()
	resp, err := adm.ctrlConn.DeleteTopics(&protocol.DeleteTopicsRequest{
		Topics: topics,
	})
	if err != nil { return err }
	for _, topicErrCode := range resp.TopicErrorCodes {
		if topicErrCode.ErrorCode != protocol.ErrNone.Code() &&
			topicErrCode.ErrorCode != protocol.ErrRequestTimedOut.Code() {
			err = protocol.Errs[topicErrCode.ErrorCode]
			return err
		}
	}
	return nil
}

//return map indexed by topic name and bool values indicating "internal" topic or not
func (adm *AdminClient) ListTopics(opt *Options) (res map[string]bool, err error) {
	adm.Lock()
	defer adm.Unlock()
	resp, err := adm.conn.Metadata(&protocol.MetadataRequest{
		APIVersion: 1,
		Topics: nil,
	})
	if err != nil {
		return nil, err
	}
	res = make(map[string]bool)
	for _, tm := range resp.TopicMetadata {
		res[tm.Topic]=tm.IsInternal
	}
	return
}

type TopicPartitionInfo struct {
	ID int32
	Leader int32
	Replicas []int32
	Isr []int32
}

type TopicDescription struct {
	Name string
	Internal bool
	Partitions []*TopicPartitionInfo
}

func (adm *AdminClient) DescribeTopics(topics []string, opt *Options) (topinfo []*TopicDescription, err error) {
	adm.Lock()
	defer adm.Unlock()
	resp, err := adm.conn.Metadata(&protocol.MetadataRequest{
		APIVersion: 1,
		Topics: topics,
	})
	if err != nil {
		return nil, err
	}
	for _, tm := range resp.TopicMetadata {
		ti := &TopicDescription{
			Name:tm.Topic,
			Internal:tm.IsInternal,
		}
		var parts []*TopicPartitionInfo
		for _, pm := range tm.PartitionMetadata {
			tp := &TopicPartitionInfo{
				ID: pm.ParititionID,
				Leader: pm.Leader,
			}
			tp.Replicas = append(tp.Replicas,pm.Replicas...)
			tp.Isr = append(tp.Isr, pm.ISR...)
			parts = append(parts,tp)
		}
		ti.Partitions = parts
		topinfo = append(topinfo,ti)
	}
	return
}

type Node struct {
	ID int32
	Host   string
	Port   int32
	// unsupported: Rack *string
}
func (n *Node) Addr() string {
	return fmt.Sprintf("%s:%d",n.Host,n.Port)
}
func (n *Node) String() string {
	return fmt.Sprintf("%d-%s:%d",n.ID,n.Host,n.Port)
}

type ClusterInfo struct {
	Nodes []*Node
	Controller *Node
}
func (ci *ClusterInfo) String() string {
	return fmt.Sprintf("Controller: %v\nNodes: %v\n",ci.Controller,ci.Nodes)
}

func (adm *AdminClient) DescribeCluster(opt *Options) (cinfo *ClusterInfo, err error) {
	adm.Lock()
	defer adm.Unlock()
	resp, err := adm.conn.Metadata(&protocol.MetadataRequest{
		APIVersion: 1,
		Topics: []string{},
		AllowAutoTopicCreation: true,
	})
	if err != nil {
		return nil, err
	}
	cinfo = &ClusterInfo{}
	for _, b := range resp.Brokers {
		n := &Node{b.NodeID,b.Host,b.Port}
		cinfo.Nodes = append(cinfo.Nodes, n)
		if resp.ControllerID == b.NodeID {
			cinfo.Controller = n
		}
	}
	return cinfo, nil
}

type APIVersion struct {
	APIKey     int16
	MinVersion int16
	MaxVersion int16
}

func (adm *AdminClient) APIVersions(nodes []string, opt *Options) (versionInfo map[string][]APIVersion, err error) {
	if len(nodes) == 0 {
		return nil, errors.New("Invalid broker nodes info")
	}
	versionInfo = make(map[string][]APIVersion)
	adm.Lock()
	defer adm.Unlock()
	for _, nname := range nodes {
		nconn, err := Dial("tcp", nname)
		if err!=nil {
			fmt.Printf("failed to dial broker node %s\n",nname)
			continue
		}
		fmt.Println("connect to broker: ",nname)

		resp, err := nconn.APIVersions(&protocol.APIVersionsRequest{
			APIVersion: 1,
		})
		nconn.Close()

		if err != nil {
			fmt.Printf("failed to retrieve api version info %v\n",err)
			continue
		}
		fmt.Printf("%s verinfo: %v error: %v\n",nname,resp.APIVersions,resp.ErrorCode)
		for _, v := range resp.APIVersions {
			versionInfo[nname] = append(versionInfo[nname], APIVersion{v.APIKey,v.MinVersion,v.MaxVersion})
		}
	}
	return versionInfo, nil
}
