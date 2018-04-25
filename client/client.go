package client
import (
	"errors"
	"time"
	"fmt"
	"strings"
	"strconv"
	"math/rand"
	
	"github.com/travisjeffery/jocko/protocol"
)

//simple client
type Client struct {
	bootstrapBrokers []string
	//accumulate discovered cluster state to improve performance
	//eg. load balance, reuse conn
	//only used to speedup query, results always from querying cluster
	clusterState *ClusterState //cluster's nodes, groups
	conns map[string]*Conn //connections to brokers
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

func NewClient(bootBrokers []string, cfg *ClientConfig, state ...*ClusterState) (cli *Client, err error) {
	rand.Seed(time.Now().UnixNano())
	cli = &Client{bootstrapBrokers:bootBrokers,config:cfg}
	if len(state)>0 {
		cli.clusterState = state[0]
	} else {
		cli.clusterState = &ClusterState{}
	}
	cli.conns = make(map[string]*Conn)
	//if no init bootstrap brokers defined
	//use brokers discovered in past
	if len(bootBrokers)==0 { 
		if len(state)>0 && len(state[0].Nodes)>0 {
			for _,b := range state[0].Nodes {
				bootBrokers = append(bootBrokers,b.Addr())
			}
		}
		if len(bootBrokers)==0 {
			err = errors.New("need to specify bootstrap brokers with --brokers or -b")
			return
		}
	}
	var conn *Conn
	visited := make(map[string]bool)
	for len(bootBrokers)>0 && len(visited)<len(bootBrokers) {
		broker := bootBrokers[rand.Intn(len(bootBrokers))]
		if !visited[broker] {
			visited[broker]=true
			conn, err = Dial("tcp",broker)
			if err == nil {
				cli.conns[broker]=conn
				break
			}
		}
	}
	//fmt.Printf("bootBroker:%v\n%v\n",bootBrokers,cli.conns)
	if err != nil { //failed to conn to any broker
		return cli, errors.New(fmt.Sprintf("fail to connect to any of %v\n",bootBrokers))
	}
	return cli, nil
}

func (cli *Client) Close() {
	for _, conn := range cli.conns {
		conn.Close()
	}
}

//pick a random conn for next command
func (cli *Client) conn() (conn *Conn) {
	var broker string
	if len(cli.conns)==1 {
		for broker,conn = range cli.conns {
			break
		}
	} else {
		//use Go map iteration randomness to pick a random broker
		var brokers []string
		for b,_ := range cli.conns { brokers = append(brokers,b) }
		broker = brokers[rand.Intn(len(brokers)*100)/100]
		conn = cli.conns[broker]
	}
	fmt.Printf("connect to %s for command\n",broker)
	return conn
}

//return conn to ctrl broker
func (cli *Client) ctrlConn() *Conn {
	if cli.clusterState.Controller == nil || cli.conns[cli.clusterState.Controller.Addr()]==nil {
		if err := cli.connectController(); err != nil {
			return nil
		}
	}
	return cli.conns[cli.clusterState.Controller.Addr()]
}

func (cli *Client) connect(broker string) (conn *Conn, err error) {
	if len(broker) > 0 {
		conn = cli.conns[broker]
		if conn == nil {
			conn, err = Dial("tcp", broker)
			if err!=nil {
				fmt.Printf("failed to dial broker node %s\n",broker)
				return
			}
			cli.conns[broker]=conn
		}
	} else {
		err = errors.New(fmt.Sprintf("Invalid broker addr: %v",broker))
	}
	return
}

//connect to cluster controller broker
//for create_topic/delete_topic, need conn to controller
func (cli *Client) connectController() (err error) {
	if len(cli.conns) == 0 {
		return errors.New("Admin client is not connected to any broker")
	} else if cli.clusterState.Controller!=nil && cli.conns[cli.clusterState.Controller.Addr()] != nil {
		return
	}
	if cli.clusterState.Controller == nil {
		_, err := cli.DescribeCluster(DefaultOptions)
		if err != nil {
			return err
		}
	}
	ctrlBroker := cli.clusterState.Controller.Addr()

	if cli.conns[ctrlBroker] != nil {
		return
	} else {
		ctrlConn, err := Dial("tcp", ctrlBroker)
		if err!=nil {
			return errors.New(fmt.Sprintf("failed to dial controller broker %s\n",ctrlBroker))
		}
		cli.conns[ctrlBroker] = ctrlConn
		fmt.Println("connect to controller broker: ",ctrlBroker)
	}
	return nil
}

//return map indexed by topic name and bool values indicating "internal" topic or not
func (cli *Client) ListTopics(opt *Options) (res map[string]bool, err error) {
	resp, err := cli.conn().Metadata(&protocol.MetadataRequest{
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
	cli.clusterState.updateNodes(resp)
	return
}

type TopicPartitionInfo struct {
	ID int32           `json:"id"`
	Leader int32       `json:"leader"`
	Replicas []int32   `json:"replicas"`
	Isr []int32        `json:"isr"`
}

type TopicDescription struct {
	Name string                        `json:"name"`
	Internal bool                      `json:"internal"`
	Partitions []*TopicPartitionInfo   `json:"partitions"`
}

func (cli *Client) DescribeTopics(topics []string, opt *Options) (topinfo []*TopicDescription, err error) {
	resp, err := cli.conn().Metadata(&protocol.MetadataRequest{
		APIVersion: 1,
		Topics: topics,
	})
	if err != nil {
		return nil, err
	}
	cli.clusterState.updateNodes(resp)
	for _, tm := range resp.TopicMetadata {
		//broker return non-exist topic without partition data with no error
		//skip it
		if len(tm.PartitionMetadata) == 0 { continue }
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
	ID int32        `json:"id"`
	Host   string   `json:"host"`
	Port   int32    `json:"port"`
	// unsupported: Rack *string
}
func (n *Node) Addr() string {
	return fmt.Sprintf("%s:%d",n.Host,n.Port)
}
func (n *Node) String() string {
	return fmt.Sprintf("%d-%s:%d",n.ID,n.Host,n.Port)
}

type ClusterInfo struct {
	Controller *Node   `json:"controller"`
	Nodes []*Node      `json:"nodes"`
}

func (ci *ClusterInfo) String() string {
	return fmt.Sprintf("Controller: %v\nNodes: %v\n",ci.Controller,ci.Nodes)
}

func (cli *Client) DescribeCluster(opt *Options) (cinfo *ClusterInfo, err error) {
	resp, err := cli.conn().Metadata(&protocol.MetadataRequest{
		APIVersion: 1,
		Topics: []string{},
		AllowAutoTopicCreation: true,
	})
	if err != nil {
		return nil, err
	}
	cli.clusterState.updateNodes(resp)
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

type GroupMember struct {
	ClientID              string     `json:"clientID"`
	ClientHost            string     `json:"host"`
	//GroupMemberMetadata   []byte
	//GroupMemberAssignment []byte
}

type Group struct {
	GroupID string       `json:"id"`
	State        string  `json:"state"`
	ProtocolType string  `json:"protocolType"`
	Protocol     string  `json:"protocol"`
	GroupMembers map[string]*GroupMember `json:"groupMembers"`
}

func (cli *Client) ListGroups(opt *Options) (nodeGroupIds map[string][]string, err error) {
	if cli.clusterState.Nodes == nil {
		_, err := cli.DescribeCluster(DefaultOptions)
		if err != nil {
			return nil,err
		}
	}
	nodeGroupIds = make(map[string][]string)
	for _, n := range cli.clusterState.Nodes {
		conn, err := cli.connect(n.Addr())
		if err!= nil {
			continue
		}
		resp, err := conn.ListGroups(&protocol.ListGroupsRequest{
			APIVersion: 0,
		})
		if err == nil && resp.ErrorCode != protocol.ErrNone.Code() {
			err = protocol.Errs[resp.ErrorCode]
		}
		if err != nil {
			fmt.Printf("fail to get group info from %v: %v\n",n,err)
			continue
		}
		var gIds []string
		for _, g := range resp.Groups {
			gIds = append(gIds, g.GroupID)
		}
		nodeGroupIds[n.Addr()]=gIds
	}
	cli.clusterState.GroupIds = nodeGroupIds
	return
}

func (cli *Client) DescribeGroups(groupIds []string, opt *Options) (groups []*Group, err error) {
	nodeGroupIds := cli.clusterState.GroupIds
	if nodeGroupIds == nil { //not listGroup/discovered
		nodeGroupIds,err = cli.ListGroups(opt)
	}
	if err != nil {
		return nil, err
	}
	gidmap := make(map[string]bool)
	for _,g:=range groupIds { gidmap[g]=true }
	for n, gIds := range nodeGroupIds {
		var ids []string
		for _, gid := range gIds {
			if gidmap[gid] {
				ids = append(ids,gid)
				delete(gidmap,gid)
			}
		}
		if len(ids)>0 {
			conn, err := cli.connect(n)
			if err!= nil {
				continue
			}

			resp, err := conn.DescribeGroups(&protocol.DescribeGroupsRequest{
				APIVersion: 0,
				GroupIDs: ids,
			})
			if err != nil {
				fmt.Printf("fail to describeGroups from %v:%v\n",n,err)
				continue
			}
			for _,g := range resp.Groups {
				if g.ErrorCode != protocol.ErrNone.Code() {
					continue
				}
				grp := &Group{g.GroupID,g.State,g.ProtocolType,g.Protocol,make(map[string]*GroupMember)}
				for mid,m := range g.GroupMembers {
					mb := &GroupMember{m.ClientID,m.ClientHost}
					grp.GroupMembers[mid] =  mb
				}
				groups = append(groups,grp)
			}
		}
		if len(gidmap)==0 {
			break
		}
	}

	return
}


type NodeTopicInfo struct {
	TopicName string    `json:"topicName"`
	PartitionID int32   `json:"partitionId"`
	IsInternal bool     `json:"internal"`
	IsLeader bool       `json:"leader"`
	Replicas []int32    `json:"replicas"`
	Isr []int32         `json:"isr"`
}

func (cli *Client) DescribeNodes(nodes []string, opt *Options) (info map[string][]*NodeTopicInfo, err error) {
	resp, err := cli.conn().Metadata(&protocol.MetadataRequest{
		APIVersion: 1,
		Topics: nil,
		AllowAutoTopicCreation: true,
	})
	if err != nil {
		return nil, err
	}
	cli.clusterState.updateNodes(resp)
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
