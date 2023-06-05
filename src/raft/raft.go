package raft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// 这里会影响日志同步
var HeartBeatTimeout = 30 * time.Millisecond

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int          // 当前任期
	votedFor    int          // 要投票给谁
	logs        []logEntries // 日志条目
	commitIndex int          // 已经提交的日志最大索引
	lastApplied int          // 已经执行完了日志的最大索引
	nextIndexs  []int        //Leader 节点对于每个 Follower 节点的下一次要复制的日志条目的索引
	matchIndex  []int
	applyChan   chan ApplyMsg // 已经执行完了的消息channel
	myStatus    Status        // 当前状态
	voteTimeout time.Duration // 选举超时时间，随机
	timer       *time.Ticker  // timer
	applyCond   *sync.Cond
}

type Status int64

type logEntries struct {
	Term    int
	Command interface{}
}

const (
	Follower Status = iota
	Candidate
	Leader
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.myStatus == Leader
	rf.mu.Unlock()
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []logEntries
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&logs) != nil {
		fmt.Println("Readpersist error")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
	}

}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	if rf.killed() {
		return
	}

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int //候选人的任期
	CandidateId  int // 投给哪位候选人
	LastLogIndex int // 候选人最新日志条目索引
	LastLogTerm  int //候选人最新日志条目的任期
}

type VoteErr int64

const (
	Nil                VoteErr = iota //投票过程无错误
	VoteReqOutofDate                  //投票消息过期
	CandidateLogTooOld                //候选人Log不够新
	VotedThisTerm                     //本Term内已经投过票
	RaftKilled                        //Raft程已终止
)

const (
	AppendErr_Nil          AppendEntriesErr = iota // 无错误
	AppendErr_LogsNotMatch                         // Append操作log不匹配
	AppendErr_ReqOutofDate                         // Append操作请求过期
	AppendErr_ReqRepeat                            // Append请求重复
	AppendErr_Commited                             // Append log已经commit了
	AppendErr_RaftKilled                           // Raft程序终止
)

type AppendEntriesErr int64

type AppendEntriesArgs struct {
	Term         int // leader的任期
	LeaderId     int // leaderid
	PrevLogIndex int // nextIndexs前一个index
	PreLogTerm   int
	Logs         []logEntries // 需要添加的日志，如果为空，那么就是心跳操作
	LeaderCommit int          // leader 已经commit了的log index
	LogIndex     int
}

type AppendEntriesReply struct {
	Term               int
	Success            bool
	AppendErr          AppendEntriesErr
	FirstNotMatchIndex int // 当前term中，第一个没有被commit的索引
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // 候选人的任期
	VoteGranted bool // 如果返回true，则表示候选人获得了选票
	VoteError   VoteErr
}

// 投票具体处理
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// 当线程被kill
	if rf.killed() {
		reply.Term = -1
		reply.VoteGranted = false
		reply.VoteError = RaftKilled
		return
	}

	rf.mu.Lock()

	// 当请求的任期小于当前任期，直接返回当前节点的任期
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		reply.VoteError = VoteReqOutofDate
		rf.mu.Unlock()
		return
	}
	// 当请求的任期大于当前节点的任期，当前节点转为 follwer
	if args.Term > rf.currentTerm {
		rf.myStatus = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}

	// 如果候选人的日志最后任期小于当前日志最后任期，那么当前节点比候选人的任期还新
	if args.LastLogTerm < rf.logs[len(rf.logs)-1].Term {
		reply.Term = rf.currentTerm
		reply.VoteError = CandidateLogTooOld
		reply.VoteGranted = false
		rf.mu.Unlock()
		return
	}
	// 任期相同，但是当前节点的日志比请求的日志长，这说明当前节点更新
	if args.LastLogTerm == rf.logs[len(rf.logs)-1].Term && args.LastLogIndex < len(rf.logs)-1 {
		reply.Term = rf.currentTerm
		reply.VoteError = CandidateLogTooOld
		reply.VoteGranted = false
		rf.mu.Unlock()
		return
	}

	// 当前任期等于请求的任期
	if rf.currentTerm == args.Term {

		reply.Term = args.Term

		// 已经投过相同的候选人了，直接转成 Follower
		if rf.votedFor == args.CandidateId {
			rf.myStatus = Follower
			rf.timer.Reset(rf.voteTimeout)
			reply.VoteGranted = true
			reply.VoteError = VotedThisTerm
			rf.persist()
			rf.mu.Unlock()
			return
		}

		// 已经投过别人了
		if rf.votedFor != -1 {
			reply.VoteGranted = false
			reply.VoteError = VotedThisTerm
			rf.mu.Unlock()
			return
		}
	}

	rf.currentTerm = args.Term
	rf.timer.Reset(rf.voteTimeout)
	rf.votedFor = args.CandidateId
	rf.myStatus = Follower
	rf.persist()
	reply.Term = rf.currentTerm
	reply.VoteGranted = true
	reply.VoteError = Nil

	rf.mu.Unlock()
}

// 投票结果处理，此方法是候选人才会执行的
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, votedNum *int) bool {
	// 这里一定要判断，不然走后面的请求会有问题
	if rf.killed() {
		return false
	}
	// 一直请求，直到成功
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	for !ok {
		// 这里的判断非常重要
		if rf.killed() {
			return false
		}
		ok = rf.peers[server].Call("Raft.RequestVote", args, reply)

		if ok {
			break
		}
	}

	if rf.killed() {
		return false
	}

	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return false
	}
	rf.mu.Unlock()

	switch reply.VoteError {
	case VoteReqOutofDate:
		rf.mu.Lock()
		rf.myStatus = Follower
		// 请求投票结束需要重置下投票超时时间
		rf.timer.Reset(rf.voteTimeout)
		// 返回的任期大于当前候选人的任期(此时其实已经是Follower了)
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.persist()
		}
		rf.mu.Unlock()
	case CandidateLogTooOld:
		rf.mu.Lock()
		rf.myStatus = Follower
		rf.timer.Reset(rf.voteTimeout)
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.persist()
		}
		rf.mu.Unlock()
	case Nil, VotedThisTerm:
		rf.mu.Lock()

		if reply.VoteGranted && reply.Term == rf.currentTerm && *votedNum <= len(rf.peers)/2 {
			*votedNum++
		}

		if *votedNum > len(rf.peers)/2 {
			// 重置投票数
			*votedNum = 0
			// 如果已经是leader，直接返回
			if rf.myStatus == Leader {
				rf.mu.Unlock()
				return ok
			}
			// 投票数过半，当选leader
			rf.myStatus = Leader
			// 创建所有peers的nextIndexs
			rf.nextIndexs = make([]int, len(rf.peers))
			// nextIndexs 这个数组指的是每一个节点的日志的下一个索引，所以都是为len(rf.logs) ，而当前的最大索引应该是len(rf.logs) - 1
			// 将所有的peer的nextindex都设置为当前leader的日志长度
			for i, _ := range rf.nextIndexs {
				rf.nextIndexs[i] = len(rf.logs)
			}
			rf.timer.Reset(HeartBeatTimeout)
		}
		rf.persist()
		rf.mu.Unlock()

	case RaftKilled:
		return false
	}

	return ok
}

// 日志具体处理
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	if rf.killed() {
		reply.Success = false
		reply.AppendErr = AppendErr_RaftKilled
		reply.Term = -1
		return
	}

	rf.mu.Lock()

	// 如果该请求过期了
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.AppendErr = AppendErr_ReqOutofDate
		reply.FirstNotMatchIndex = -1
		rf.mu.Unlock()
		return
	}
	// 更新rf
	rf.currentTerm = args.Term
	rf.votedFor = args.LeaderId
	rf.myStatus = Follower
	rf.timer.Reset(rf.voteTimeout)
	rf.persist()

	// rf日志长度不足，或者 相同索引的日志的任期不同
	if args.PrevLogIndex >= len(rf.logs) || rf.logs[args.PrevLogIndex].Term != args.PreLogTerm {
		reply.Term = rf.currentTerm
		reply.AppendErr = AppendErr_LogsNotMatch
		reply.Success = false
		reply.FirstNotMatchIndex = rf.lastApplied + 1
		rf.mu.Unlock()
		return
	}

	// 如果rf.lastApplied > args.prevLogIndex ，那么就是说本节点已经应用的日志已经大于了请求过来的日志，所以rf才是最新的
	if rf.lastApplied > args.PrevLogIndex {
		reply.Term = rf.currentTerm
		reply.AppendErr = AppendErr_Commited
		reply.Success = false
		reply.FirstNotMatchIndex = rf.lastApplied + 1
		rf.mu.Unlock()
		return
	}

	// 拼接日志
	if args.Logs != nil {
		rf.logs = rf.logs[:args.PrevLogIndex+1]
		rf.logs = append(rf.logs, args.Logs...)
	}

	reply.Term = rf.currentTerm
	reply.Success = true
	reply.AppendErr = AppendErr_Nil
	reply.FirstNotMatchIndex = -1
	rf.persist()
	rf.mu.Unlock()
}

// 日志结果处理
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply, appendNum *int) bool {
	if rf.killed() {
		return false
	}

	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	for !ok {
		// 这里的判断非常重要
		if rf.killed() {
			return false
		}
		ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)

		if ok {
			break
		}
	}

	if rf.killed() {
		return false
	}

	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return false
	}
	rf.mu.Unlock()

	switch reply.AppendErr {
	case AppendErr_Nil:
		rf.mu.Lock()
		if reply.Success && reply.Term == rf.currentTerm && *appendNum <= len(rf.peers)/2 {
			*appendNum++
		}
		// args.LogIndex 是logs的最新的索引。rf.nextIndexs[server]是server这个服务器的最新下一个索引
		if rf.nextIndexs[server] >= args.LogIndex+1 {
			rf.mu.Unlock()
			return ok
		}

		rf.nextIndexs[server] = args.LogIndex + 1

		// 收到了一半的回应
		if *appendNum > len(rf.peers)/2 {
			*appendNum = 0
			// 当前的任期跟此次请求的任期不同
			if rf.logs[args.LogIndex].Term != rf.currentTerm {
				rf.mu.Unlock()
				return false
			}
			// 提交的比已经应用的多，所以要将提交的命令执行
			//log.Printf("[%v] rf.lastApplied == %v, args.LeaderCommit == %v, args.LogIndex == %v", rf.me, rf.lastApplied, args.LeaderCommit, args.LogIndex)
			// 这一块感觉没必要存在，LogIndex 一般都是大于 leaderCommit 的
			// 但是实际也不合理，这里应该是应用到跟 leaderCommit 相同的
			//for rf.lastApplied < args.LeaderCommit {
			//	rf.lastApplied++
			//	applyMsg := ApplyMsg{
			//		CommandValid: true,
			//		Command:      rf.logs[rf.lastApplied].Command,
			//		CommandIndex: rf.lastApplied,
			//	}
			//	rf.applyChan <- applyMsg
			//	rf.commitIndex = rf.lastApplied
			//}
			//
			//for rf.lastApplied < args.LogIndex {
			//	rf.lastApplied++
			//	applyMsg := ApplyMsg{
			//		CommandValid: true,
			//		Command:      rf.logs[rf.lastApplied].Command,
			//		CommandIndex: rf.lastApplied,
			//	}
			//	// 应用到状态机中
			//	//rf.mu.Unlock()
			//	rf.applyChan <- applyMsg
			//	//rf.mu.Lock()
			//	rf.commitIndex = rf.lastApplied
			//}
			if args.LogIndex > rf.commitIndex {
				if args.LogIndex > len(rf.logs)-1 {
					rf.commitIndex = len(rf.logs) - 1
				} else {
					rf.commitIndex = args.LogIndex
				}
				rf.applyCond.Broadcast()
			}
		}
		rf.persist()
		rf.mu.Unlock()
	case AppendErr_ReqOutofDate:
		rf.mu.Lock()
		rf.myStatus = Follower
		rf.timer.Reset(rf.voteTimeout)
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.persist()
		}
		rf.mu.Unlock()
	case AppendErr_LogsNotMatch:
		rf.mu.Lock()
		// 请求时的任期 和 当前 leader 的任期不一定会相同
		if rf.currentTerm != args.Term {
			rf.mu.Unlock()
			return false
		}
		rf.nextIndexs[server] = reply.FirstNotMatchIndex
		rf.mu.Unlock()

	//case AppendErr_ReqRepeat:
	//	rf.mu.Lock()
	//	if reply.Term > rf.currentTerm {
	//		rf.myStatus = Follower
	//		rf.votedFor = -1
	//		rf.currentTerm = reply.Term
	//		rf.timer.Reset(rf.voteTimeout)
	//		rf.persist()
	//	}
	//	rf.mu.Unlock()
	case AppendErr_Commited:
		rf.mu.Lock()
		if args.Term != rf.currentTerm {
			rf.mu.Unlock()
			return false
		}
		rf.nextIndexs[server] = reply.FirstNotMatchIndex
		rf.mu.Unlock()

	case AppendErr_RaftKilled:
		return false
	}
	return ok
}

// 将 commit 的日志 apply 到状态机上
func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for !rf.killed() {
		//log.Printf("[%v]  rf.lastApplied == %v, rf.commitIndex == %v", rf.me, rf.lastApplied, rf.commitIndex)
		if rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.logs[rf.lastApplied].Command,
				CommandIndex: rf.lastApplied,
			}
			// 应用到状态机中
			rf.mu.Unlock()
			rf.applyChan <- applyMsg
			rf.mu.Lock()
		} else {
			rf.applyCond.Wait()
		}
	}
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	rf.mu.Lock()
	if rf.killed() {
		rf.mu.Unlock()
		return index, term, isLeader
	}
	isLeader = rf.myStatus == Leader
	// 如果不是leader，返回false
	if !isLeader {
		rf.mu.Unlock()
		return index, term, false
	}
	newLog := logEntries{
		Command: command,
		Term:    rf.currentTerm,
	}
	rf.logs = append(rf.logs, newLog)
	index = len(rf.logs) - 1
	term = rf.currentTerm
	rf.persist()
	rf.mu.Unlock()
	return index, term, isLeader
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.mu.Lock()
	rf.timer.Stop()
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {

	for rf.killed() == false {
		select {
		case <-rf.timer.C:
			if rf.killed() {
				return
			}
			rf.mu.Lock()
			curStatus := rf.myStatus
			switch curStatus {
			case Follower:
				curStatus = Candidate
				fallthrough
			case Candidate:
				rf.currentTerm += 1
				rf.votedFor = rf.me
				rf.voteTimeout = time.Duration(rand.Intn(150)+200) * time.Millisecond
				rf.timer.Reset(rf.voteTimeout)
				rf.persist()
				votedNum := 1 // 获取到的投票数，自己投自己一票
				for i, _ := range rf.peers {
					if i == rf.me { // 自己就不用请求了
						continue
					}
					args := &RequestVoteArgs{
						Term:         rf.currentTerm,
						CandidateId:  rf.me,
						LastLogIndex: len(rf.logs) - 1,
						LastLogTerm:  rf.logs[len(rf.logs)-1].Term,
					}
					reply := new(RequestVoteReply)
					go rf.sendRequestVote(i, args, reply, &votedNum)
				}
			case Leader:
				//log.Printf("rf.me in == %v, rf.myStatus == %v", rf.me, rf.myStatus)
				appendNum := 1
				// 发起了心跳就更新心跳时间
				rf.timer.Reset(HeartBeatTimeout)
				//log.Printf("rf.me = %v", rf.me)
				for i, _ := range rf.peers {
					if i == rf.me {
						continue
					}
					// 请求参数
					args := &AppendEntriesArgs{
						Term:         rf.currentTerm, // 当前leader的任期
						LeaderId:     rf.me,          // 当前的leaderid必然是自己
						PrevLogIndex: 0,              // log中nextIndexs的前一个index
						PreLogTerm:   0,
						Logs:         nil,              // 如果不携带logs，就是心跳
						LeaderCommit: rf.commitIndex,   // leader已经commit的index
						LogIndex:     len(rf.logs) - 1, // 当前leader日志的最大索引
					}
					// 这里注意是为了处理 follower 节点的日志异常情况
					// 其他副本节点的nextIndexs大于 leader 的日志长度，此时需要将其他副本节点的日志全部统一成 leader 的日志
					for rf.nextIndexs[i] > 0 {
						args.PrevLogIndex = rf.nextIndexs[i] - 1
						if args.PrevLogIndex >= len(rf.logs) {
							rf.nextIndexs[i]--
							continue
						}
						args.PreLogTerm = rf.logs[args.PrevLogIndex].Term
						break
					}

					// 确认日志是否有新增
					if rf.nextIndexs[i] < len(rf.logs) {
						args.Logs = make([]logEntries, args.LogIndex+1-rf.nextIndexs[i])
						// 数组取范围是左闭右开
						copy(args.Logs, rf.logs[rf.nextIndexs[i]:args.LogIndex+1])
					}
					reply := new(AppendEntriesReply)
					go rf.sendAppendEntries(i, args, reply, &appendNum)
				}
			}
			rf.mu.Unlock()
		}
	}
}

func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndexs = nil
	rf.matchIndex = nil
	rf.logs = []logEntries{{0, nil}}
	rf.myStatus = Follower
	rand.Seed(time.Now().UnixNano())
	rf.voteTimeout = time.Duration(rand.Intn(150)+200) * time.Millisecond
	rf.timer = time.NewTicker(rf.voteTimeout)
	rf.applyChan = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.applier()
	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
