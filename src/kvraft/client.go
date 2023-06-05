package kvraft

import (
	"6.824/labrpc"
	"log"
	"sync"
	"sync/atomic"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	mu       sync.Mutex
	leaderId int   // 当前集群中 leader 的 id
	clientId int64 // 客户端 id
	seqId    int64 // 请求的序列号
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clientId = nrand()
	ck.leaderId = 0
	ck.seqId = 0
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	args := &GetArgs{
		Key:      key,
		ClientId: ck.clientId,
		SeqId:    atomic.AddInt64(&ck.seqId, 1),
	}

	// You will have to modify this function.
	for {
		reply := &GetReply{}
		leaderId := ck.leaderId
		ok := ck.servers[leaderId].Call("KVServer.Get", args, reply)
		log.Printf("[ClientId:%v] [%v] [leaderId:%v] Get reply.ERR is %v and reply.Value = %v", args.ClientId, args.SeqId, leaderId, reply.Err, reply.Value)
		if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
			//log.Printf("this is OK")
			return reply.Value
		} else {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		}
		time.Sleep(100)
	}
	return ""
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := &PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientId: ck.clientId,
		SeqId:    atomic.AddInt64(&ck.seqId, 1),
	}
	for {

		reply := &PutAppendReply{}
		leaderId := ck.leaderId
		//log.Printf("[ClientId:%v] [%v] [leaderId:%v]", ck.clientId, ck.seqId, leaderId)
		ok := ck.servers[leaderId].Call("KVServer.PutAppend", args, reply)
		log.Printf("[ClientId:%v] [%v] [leaderId:%v] %v reply.ERR is %v", args.ClientId, args.SeqId, leaderId, args.Op, reply.Err)
		if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
			break
		} else {
			ck.leaderId = (leaderId + 1) % len(ck.servers)
		}
		time.Sleep(150 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")

}
