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
}

func NewAdminClient(bootBrokers []string, cfg *ClientConfig, state ...*ClusterState) (adminCli *AdminClient, err error) {
	adminCli = &AdminClient{}
	adminCli.Client, err = NewClient(bootBrokers,cfg,state...)
	return adminCli, err
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
	resp, err := adm.ctrlConn().CreateTopics(&protocol.CreateTopicRequests{
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
	resp, err := adm.ctrlConn().DeleteTopics(&protocol.DeleteTopicsRequest{
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

type APIVersion struct {
	APIKey     int16    `json:"key"`
	MinVersion int16    `json:"minVersion"`
	MaxVersion int16    `json:"maxVersion"`
}

func (adm *AdminClient) APIVersions(nodes []string, opt *Options) (versionInfo map[string][]APIVersion, err error) {
	if len(nodes) == 0 {
		return nil, errors.New("Invalid broker nodes info")
	}
	versionInfo = make(map[string][]APIVersion)
	for _, nname := range nodes {
		nconn, err := adm.connect(nname)
		if err != nil {
			continue
		}

		resp, err := nconn.APIVersions(&protocol.APIVersionsRequest{
			APIVersion: 0,
		})

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

