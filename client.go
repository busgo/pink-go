package client

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/busgo/pink-go/etcd"
	"log"
	"net"
	"strings"
	"sync"
	"time"
)

const (
	InstancePath               = "/pink/client/%s/instances/%s"
	ExecuteSnapshotPath        = "/pink/execute/snapshots/%s/%s/"
	ExecuteSnapshotHistoryPath = "/pink/execute/history/snapshots/"
	InstanceTTL                = 5
)

const (
	Init int32 = iota
	Doing
	Success
	Fail
)

// PinkClient
type PinkClient struct {
	cli                 *etcd.Cli
	group               string
	ip                  string
	instancePath        string
	executeSnapshotPath string
	jobs                map[string]Job
	executingJobs       map[string]*ExecuteSnapshot
	rw                  sync.RWMutex
}

type ExecuteSnapshot struct {
	Id           string `json:"id"`
	JobId        string `json:"job_id"`
	Name         string `json:"name"`
	Group        string `json:"group"`
	Cron         string `json:"cron"`
	Target       string `json:"target"`
	Ip           string `json:"ip"`
	Param        string `json:"param"`
	State        int32  `json:"state"`
	BeforeTime   string `json:"before_time"`
	ScheduleTime string `json:"schedule_time"`
	StartTime    string `json:"start_time"`
	EndTime      string `json:"end_time"`
	Times        int64  `json:"times"`
	Mobile       string `json:"mobile"`
	Version      int32  `json:"version"`
	Result       string `json:"result"`
	Remark       string `json:"remark"`
}

func (es *ExecuteSnapshot) Encode() string {

	content, _ := json.Marshal(es)
	return string(content)
}

func (es *ExecuteSnapshot) Decode(content string) *ExecuteSnapshot {

	_ = json.Unmarshal([]byte(content), es)
	return es
}

func GetLocalIP() string {

	adds, err := net.InterfaceAddrs()
	if err != nil {
		return "127.0.0.1"
	}
	for _, addr := range adds {

		if ipNet, isIpNet := addr.(*net.IPNet); isIpNet && !ipNet.IP.IsLoopback() {
			if ipNet.IP.To4() != nil {
				return ipNet.IP.String()
			}
		}
	}
	return "127.0.0.1"
}

// new PinkClient
func NewPinkClient(cli *etcd.Cli, group string) *PinkClient {
	ip := GetLocalIP()
	client := &PinkClient{cli: cli, group: group, ip: ip,
		instancePath:        fmt.Sprintf(InstancePath, group, ip),
		executeSnapshotPath: fmt.Sprintf(ExecuteSnapshotPath, group, ip),
		jobs:                make(map[string]Job),
		executingJobs:       make(map[string]*ExecuteSnapshot),
		rw:                  sync.RWMutex{},
	}
	go client.lookup()
	go client.subscribeExecuteSnapshot()
	return client
}

// lookup
func (client *PinkClient) lookup() {
	leaseId := int64(0)
	leaseId = client.selfRegister(leaseId)
	response := client.cli.Watch(client.instancePath)
	log.Printf("the pink client instance %s self register watch to:%s", client.ip, client.instancePath)
	for {
		select {
		case event := <-response.KeyChangeCh:
			switch event.Event {
			case etcd.KeyDeleteChangeEvent:
				log.Printf("the pink client instance %s self register watch  to:%s key delete event:%+v", client.ip, client.instancePath, event)
				leaseId = client.selfRegister(leaseId)
			}
		}
	}
}

// self register
func (client *PinkClient) selfRegister(leaseId int64) int64 {

RETRY:
	log.Printf("the pink client instance %s self register to:%s", client.ip, client.instancePath)
	if leaseId > 0 {
		_ = client.cli.Revoke(context.Background(), leaseId)
	}
	leaseId, err := client.cli.Keepalive(context.Background(), client.instancePath, client.ip, InstanceTTL)
	if err != nil {
		time.Sleep(time.Second)
		goto RETRY
	}
	log.Printf("the pink client instance %s self register to:%s ,leaseId %d success", client.ip, client.instancePath, leaseId)
	return leaseId
}

// subscribe the job with target
func (client *PinkClient) Subscribe(target string, job Job) {
	client.rw.Lock()
	defer client.rw.Unlock()
	if strings.TrimSpace(target) == "" || job == nil {
		log.Printf("the pink client %s targe is nil or job is nil", client.instancePath)
		return
	}
	if targetJob := client.jobs[target]; targetJob != nil {
		log.Printf("the pink client %s target %s has exists", client.instancePath, target)
		return
	}
	client.jobs[target] = job
}

// subscribe execute snapshot
func (client *PinkClient) subscribeExecuteSnapshot() {

	keys, values, err := client.cli.GetWithPrefix(context.Background(), client.executeSnapshotPath)
	if err != nil {
		panic(err)
	}
	// delete
	client.dealSnapshots(keys, values)
	resp := client.cli.WatchWithPrefix(client.executeSnapshotPath)
	for {

		select {
		case ch := <-resp.KeyChangeCh:
			client.handleSnapshotChange(ch)
		}
	}
}

// deal snapshots
func (client *PinkClient) dealSnapshots(keys, values []string) {
	if len(keys) == 0 {
		return
	}
	for pos := 0; pos < len(keys); pos++ {
		value := values[pos]
		if strings.TrimSpace(value) == "" {
			continue
		}
		snapshot := new(ExecuteSnapshot).Decode(values[pos])
		if snapshot.State == Init {
			client.handleCreateSnapshot(&etcd.KeyChange{
				Key:   keys[pos],
				Value: value,
			})
		} else if snapshot.State == Doing { // doing
			snapshot.State = Fail
			snapshot.Result = "Doing State"
			client.transfer(keys[pos], snapshot)
		} else { // success fail
			client.transfer(keys[pos], snapshot)
		}

	}
}

// handle the execute snapshot change
func (client *PinkClient) handleSnapshotChange(kc *etcd.KeyChange) {

	switch kc.Event {
	case etcd.KeyCreateChangeEvent:
		client.handleCreateSnapshot(kc)
	case etcd.KeyUpdateChangeEvent:
		client.handleUpdateSnapshot(kc)
	case etcd.KeyDeleteChangeEvent:
		client.handleDeleteSnapshot(kc)
	}
}

// handle create snapshot
func (client *PinkClient) handleCreateSnapshot(kc *etcd.KeyChange) {
	if strings.TrimSpace(kc.Value) == "" {
		client.deleteSnapshot(kc.Key)
		return
	}
	snapshot := new(ExecuteSnapshot).Decode(kc.Value)
	targetJob := client.getTargetJob(kc.Key, snapshot)
	if targetJob == nil {
		log.Printf("the pink client %s target is nil  snapshot:%+v", client.instancePath, snapshot)
		return
	}
	if snapshot.State == Init {
		go client.execute(kc.Key, targetJob, snapshot)
		return
	} else if snapshot.State == Doing { //
		log.Printf("the pink client %s  state  is doing  set fail state, snapshot:%+v", client.instancePath, snapshot)
		snapshot.State = Fail
		snapshot.Result = "history snapshot"
	}
	client.transfer(kc.Key, snapshot)

}

// handle update snapshot
func (client *PinkClient) handleUpdateSnapshot(kc *etcd.KeyChange) {
	if strings.TrimSpace(kc.Value) == "" {
		client.deleteSnapshot(kc.Key)
		return
	}
	snapshot := new(ExecuteSnapshot).Decode(kc.Value)
	targetJob := client.getTargetJob(kc.Key, snapshot)
	if targetJob == nil {
		log.Printf("the pink client %s target is nil  snapshot:%+v", client.instancePath, snapshot)
		return
	}

	if snapshot.State == Init {
		go client.execute(kc.Key, targetJob, snapshot)
	}
}

// handle delete snapshot
func (client *PinkClient) handleDeleteSnapshot(kc *etcd.KeyChange) {
	log.Printf("the pink client %s handle delete snapshot key %s value %s", client.instancePath, kc.Key, kc.Value)
}

// transfer snapshot to history
func (client *PinkClient) transfer(key string, snapshot *ExecuteSnapshot) {

	targetKey := fmt.Sprintf("%s%s", ExecuteSnapshotHistoryPath, snapshot.Id)
	err := client.cli.Transfer(context.Background(), key, targetKey, snapshot.Encode())
	if err != nil {
		log.Printf("the pink client %s transfer key %s target key %s snapshot %+v fail %+v", client.instancePath, key, targetKey, snapshot, err)
		return
	}

}

// sync execute the snapshot to target job
func (client *PinkClient) execute(key string, targetJob Job, snapshot *ExecuteSnapshot) {

	s := client.checkExists(snapshot.JobId)
	if s != nil {
		log.Printf("the pink client %s execute snapshot found job id exists,snapshot:%+v", client.instancePath, snapshot)
		if s.Id != snapshot.Id {
			snapshot.State = Fail
			snapshot.Result = "Job is Doing,not allow repeat execute"
			client.transfer(key, snapshot)
		}
		return
	}

	client.addDoingSnapshotRecord(snapshot.JobId, snapshot)
	defer func() {
		client.deleteDoingSnapshotRecord(snapshot.JobId)
	}()
	now := time.Now()
	snapshot.State = Doing
	snapshot.StartTime = now.Format("2006-01-02 15:04:05")
	err := client.cli.Put(context.Background(), key, snapshot.Encode())
	if err != nil {
		log.Printf("the pink client %s update snapshot state fail,snapshot:%+v", client.instancePath, snapshot)
		return
	}
	result, err := targetJob.Execute(snapshot.Param)
	endTime := time.Now()
	durationTime := endTime.Sub(now)
	snapshot.Times = int64(durationTime / time.Second)
	snapshot.EndTime = endTime.Format("2006-01-02 15:04:05")
	if err != nil {
		snapshot.State = Fail
		snapshot.Result = err.Error()
	} else {
		snapshot.Result = result
		snapshot.State = Success
	}
	client.transfer(key, snapshot)
}

// delete  snapshot to history
func (client *PinkClient) deleteSnapshot(key string) {
	err := client.cli.Delete(context.Background(), key)
	if err != nil {
		log.Printf("the pink client %s delete key %s fail %+v", client.instancePath, key, err)
		return
	}

}

// get target job
func (client *PinkClient) getTargetJob(key string, snapshot *ExecuteSnapshot) Job {
	client.rw.RLock()
	defer client.rw.RUnlock()
	target := snapshot.Target
	targetJob := client.jobs[target]
	now := time.Now()

	if targetJob != nil {
		return targetJob
	}
	snapshot.StartTime = now.Format("2006-01-02 15:04:05")
	snapshot.EndTime = now.Format("2006-01-02 15:04:05")
	if snapshot.State == Doing || snapshot.State == Init {
		snapshot.State = Fail
		snapshot.Result = "not found the target job"
	}

	client.transfer(key, snapshot)
	return nil
}

func (client *PinkClient) addDoingSnapshotRecord(key string, snapshot *ExecuteSnapshot) {

	client.rw.Lock()
	defer client.rw.Unlock()
	client.executingJobs[key] = snapshot

}

func (client *PinkClient) deleteDoingSnapshotRecord(key string) {
	client.rw.Lock()
	defer client.rw.Unlock()
	delete(client.executingJobs, key)

}

func (client *PinkClient) checkExists(jobId string) *ExecuteSnapshot {
	client.rw.RLock()
	defer client.rw.RUnlock()
	return client.executingJobs[jobId]
}
