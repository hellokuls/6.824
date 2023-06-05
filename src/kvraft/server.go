package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType   string
	Key      string
	Value    string
	SeqId    int64
	ClientId int64
}

type CommandResponse struct {
	Err   Err
	Value string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvStore     map[string]string
	seqMap      map[int64]int64
	indexMap    map[IndexandTerm]chan CommandResponse
	lastApplied int
}

type IndexandTerm struct {
	index int
	term  int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	req := &Op{OpType: "Get", Key: args.Key, Value: "", SeqId: args.SeqId, ClientId: args.ClientId}
	reply.Err, reply.Value = kv.clientRequestHandler(*req)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	//log.Printf("PutAppend in  [ClientId: %v] [Seq: %v]", args.ClientId, args.SeqId)
	req := &Op{OpType: args.Op, Key: args.Key, Value: args.Value, SeqId: args.SeqId, ClientId: args.ClientId}
	reply.Err, _ = kv.clientRequestHandler(*req)
	//log.Printf("PutAppend out  [ClientId: %v] [Seq: %v]", args.ClientId, args.SeqId)
}

func (kv *KVServer) clientRequestHandler(cmd Op) (Err, string) {
	//log.Printf("1111111111111  [ClientId: %v] [Seq: %v]  kv.mu.Lock() == %v  kv.me == %v", cmd.ClientId, cmd.SeqId, kv.mu, kv.me)

	kv.mu.Lock()
	// put 请求不允许进行重复提交，会导致一系列问题
	if cmd.OpType != "Get" && kv.isDupliceRequest(cmd) {
		kv.mu.Unlock()
		return OK, "" // 此处应该返回上一次的结果
	}
	kv.mu.Unlock()
	// 调用 start 函数来调用 rf 集群，此时会返回是否为 leader，如果不是则返回

	index, term, leader := kv.rf.Start(cmd)
	if !leader {
		return ErrWrongLeader, ""
	}

	// 拿到 index 和 term 去获取 chan
	indexTerm := &IndexandTerm{
		term:  term,
		index: index,
	}
	kv.mu.Lock()
	ch := kv.getChanByIndexTerm(*indexTerm)
	// 不断循环获取 ch response，超时就返回报错
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		delete(kv.indexMap, *indexTerm)
		kv.mu.Unlock()
	}()
	select {
	case response := <-ch:
		return response.Err, response.Value
	case <-time.After(time.Duration(650) * time.Millisecond):
		return ErrWrongLeader, ""
	}

}

func (kv *KVServer) isDupliceRequest(cmd Op) bool {
	seqId, ok := kv.seqMap[cmd.ClientId]
	if ok && cmd.SeqId <= seqId {
		return true
	}
	return false
}
func (kv *KVServer) getChanByIndexTerm(term IndexandTerm) chan CommandResponse {

	ch, ok := kv.indexMap[term]
	if !ok {
		ch = make(chan CommandResponse, 1)
		kv.indexMap[term] = ch
	}
	return ch
}

func (kv *KVServer) applyChanel() {
	for !kv.killed() {
		for m := range kv.applyCh {
			//start := time.Now()

			if m.CommandValid {
				kv.mu.Lock()
				if m.CommandIndex <= kv.lastApplied {
					kv.mu.Unlock()
					continue
				}
				op := m.Command.(Op)
				kv.lastApplied = m.CommandIndex
				var response CommandResponse
				if op.OpType != "Get" && kv.isDupliceRequest(op) {
					response = CommandResponse{Err: OK, Value: ""}
				} else {
					kv.seqMap[op.ClientId] = op.SeqId
					switch op.OpType {
					case "Put":
						kv.kvStore[op.Key] = op.Value
						response = CommandResponse{Err: OK, Value: ""}
					case "Append":
						kv.kvStore[op.Key] += op.Value
						response = CommandResponse{Err: OK, Value: ""}
					case "Get":
						if value, ok := kv.kvStore[op.Key]; ok {
							response = CommandResponse{OK, value}
						} else {
							response = CommandResponse{Err: ErrNoKey, Value: ""}
						}
					}
				}
				//log.Printf("applyChanel %t-%d in lock 1", m.CommandValid, m.CommandIndex)
				term, leader := kv.rf.GetState()
				//log.Printf("applyChanel %t-%d in lock 2", m.CommandValid, m.CommandIndex)
				if leader {
					indexTerm := &IndexandTerm{
						index: m.CommandIndex,
						term:  term,
					}
					ch := kv.getChanByIndexTerm(*indexTerm)
					ch <- response
				}
			} else {
				log.Printf("error message is %v", m)
			}
			kv.mu.Unlock()
		}
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg, 10000)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.seqMap = make(map[int64]int64)
	kv.kvStore = make(map[string]string)
	kv.indexMap = make(map[IndexandTerm]chan CommandResponse, 1)
	kv.lastApplied = 0
	go kv.applyChanel()
	return kv
}
