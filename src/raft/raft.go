package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"math/rand"

	// "math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"

	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.

var HeartBeatTimeout = 120 * time.Millisecond

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

	nextIndex  []int
	matchIndex []int

	applyChan chan ApplyMsg // 已经执行完了的消息channel

	myStatus Status // 当前状态

	voteTimeout time.Duration // 选举超时时间，随机
	timer       *time.Ticker  // timer

}

type logEntries struct {
	Term    int
	Command interface{}
}

type Status int64

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
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.currentTerm)
	// e.Encode(rf.votedFor)
	// e.Encode(rf.logs)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }

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

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // 候选人的任期
	VoteGranted bool // 如果返回true，则表示候选人获得了选票
	VoteError   VoteErr
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	//考虑各种情况
	// 当线程被kill
	if rf.killed() {
		reply.Term = -1
		reply.VoteGranted = false
		reply.VoteError = RaftKilled
		return
	}

	rf.mu.Lock()

	// 当请求的任期小于当前任期
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		reply.VoteError = VoteReqOutofDate
		rf.mu.Unlock()
		return
	}

	if args.Term > rf.currentTerm {
		rf.myStatus = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	// 如果候选人的日志最后任期小于当前日志最后任期
	if args.LastLogTerm < rf.logs[len(rf.logs)-1].Term {
		// rf.currentTerm = args.Term // 疑问？
		reply.Term = rf.currentTerm
		reply.VoteError = CandidateLogTooOld
		reply.VoteGranted = false
		rf.mu.Unlock()
		return
	}
	// 最后的term相同，但是日志的index不同
	if args.LastLogTerm == rf.logs[len(rf.logs)-1].Term && args.LastLogIndex < len(rf.logs)-1 {
		// rf.currentTerm = args.Term // 疑问？
		reply.Term = rf.currentTerm
		reply.VoteError = CandidateLogTooOld
		reply.VoteGranted = false
		rf.mu.Unlock()
		return
	}

	// 如果当前任期等于请求的任期
	if rf.currentTerm == args.Term {

		reply.Term = args.Term

		// 已经投过相同的候选人了
		if rf.votedFor == args.CandidateId {
			rf.myStatus = Follower
			rf.timer.Reset(rf.voteTimeout)
			reply.VoteGranted = true
			reply.VoteError = VotedThisTerm
			rf.mu.Unlock()
			return
		}
		// 来自同一个任期其他候选人的请求
		if rf.votedFor != -1 {
			reply.VoteGranted = false
			reply.VoteError = VotedThisTerm
			rf.mu.Unlock()
			return
		}
	}

	// 排除掉上面的情况后，可以进行正式投票了

	rf.currentTerm = args.Term
	rf.timer.Reset(rf.voteTimeout)
	rf.votedFor = args.CandidateId
	rf.myStatus = Follower

	reply.Term = rf.currentTerm
	reply.VoteGranted = true
	reply.VoteError = Nil

	// rf.persist()
	rf.mu.Unlock()
}

type AppendEntriesErr int64

const (
	AppendErr_Nil          AppendEntriesErr = iota // 无错误
	AppendErr_LogsNotMatch                         // Append操作log不匹配
	AppendErr_ReqOutofDate                         // Append操作请求过期
	AppendErr_ReqRepeat                            // Append请求重复
	AppendErr_Commited                             // Append log已经commit了
	AppendErr_RaftKilled                           // Raft程序终止
)

type AppendEntriesArgs struct {
	Term         int // leader的任期
	LeaderId     int // leaderid
	PrevLogIndex int // nextIndex前一个index
	PreLogTerm   int
	Logs         []logEntries // 需要添加的日志，如果为空，那么就是心跳操作
	LeaderCommit int          // leader 已经commit了的log index
	LogIndex     int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	NotMatchIndex int // 当前term的第一个元素（没有被commit的元素）的index
	AppendErr     AppendEntriesErr
}

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
		rf.mu.Unlock()
		return
	}

	// 更新rf
	rf.currentTerm = args.Term
	rf.votedFor = args.LeaderId
	rf.myStatus = Follower
	rf.timer.Reset(rf.voteTimeout)

	// rf日志长度不足，或者 相同索引的日志的任期不同
	// reply false if log doesn’t contain an entry at prevLogIndexwhose term matches prevLogTerm
	if args.PrevLogIndex >= len(rf.logs) || rf.logs[args.PrevLogIndex].Term != args.PreLogTerm {
		reply.Term = rf.currentTerm
		reply.AppendErr = AppendErr_LogsNotMatch
		reply.Success = false
		rf.mu.Unlock()
		return
	}

	// 如果rf.lastApplied > args.prevLogIndex ，那么就是说rf副本中的最后一个日志已经大于了请求过来的日志，所以rf才是最新的
	if rf.lastApplied > args.PrevLogIndex {
		reply.Term = rf.currentTerm
		reply.AppendErr = AppendErr_Commited
		reply.Success = false
		rf.mu.Unlock()
		return
	}

	// 拼接日志
	if args.Logs != nil {
		rf.logs = rf.logs[:args.PrevLogIndex+1]
		// DPrintf("rf.logs len is %d", len(rf.logs))
		rf.logs = append(rf.logs, args.Logs...)
		// DPrintf("rf.logs added len is %d", len(rf.logs))
	}

	// 提交的比已经应用的多，所以要将提交的命令执行
	for rf.lastApplied < args.LeaderCommit {
		rf.lastApplied++
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      rf.logs[rf.lastApplied].Command,
			CommandIndex: rf.lastApplied,
		}

		rf.applyChan <- applyMsg
		rf.commitIndex = rf.lastApplied
	}

	reply.Term = rf.currentTerm
	reply.Success = true
	reply.AppendErr = AppendErr_Nil
	rf.mu.Unlock()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply, appendNum *int) bool {
	if rf.killed() {
		return false
	}

	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	for !ok {
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
		// args.LogIndex 是logs的最新的索引。rf.nextIndex[server]是server这个服务器的最新下一个索引
		if rf.nextIndex[server] >= args.LogIndex+1 {
			rf.mu.Unlock()
			return ok
		}

		rf.nextIndex[server] = args.LogIndex + 1

		// 如果收到了一半的回应
		if *appendNum > len(rf.peers)/2 {
			*appendNum = 0
			if rf.logs[args.LogIndex].Term != rf.currentTerm {
				rf.mu.Unlock()
				return false
			}
			for rf.lastApplied < args.LogIndex {
				rf.lastApplied++

				applyMsg := ApplyMsg{
					CommandValid: true,
					Command:      rf.logs[rf.lastApplied].Command,
					CommandIndex: rf.lastApplied,
				}
				// 应用到状态机中
				rf.applyChan <- applyMsg
				rf.commitIndex = rf.lastApplied
			}

		}
		rf.mu.Unlock()
	case AppendErr_ReqOutofDate:
		rf.mu.Lock()
		rf.myStatus = Follower
		rf.timer.Reset(rf.voteTimeout)
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.votedFor = -1
		}
		rf.mu.Unlock()
	case AppendErr_LogsNotMatch:
		rf.mu.Lock()
		// args.Term是当前leader的任期，如果两者不相等，直接false
		if rf.currentTerm != args.Term {
			rf.mu.Unlock()
			return false
		}
		rf.nextIndex[server]--

		argsNews := &AppendEntriesArgs{
			Term:         args.Term,
			LeaderId:     rf.me,
			PrevLogIndex: 0,
			PreLogTerm:   0,
			Logs:         nil,
			LeaderCommit: args.LeaderCommit,
			LogIndex:     args.LogIndex,
		}

		for rf.nextIndex[server] > 0 {
			argsNews.PrevLogIndex = rf.nextIndex[server] - 1

			if argsNews.PrevLogIndex >= len(rf.logs) {
				rf.nextIndex[server]--
				continue
			}

			argsNews.PreLogTerm = rf.logs[argsNews.PrevLogIndex].Term
			break
		}

		if rf.nextIndex[server] < args.LogIndex+1 {
			argsNews.Logs = rf.logs[rf.nextIndex[server] : args.LogIndex+1]
		}

		reply := new(AppendEntriesReply)
		go rf.sendAppendEntries(server, argsNews, reply, appendNum)
		rf.mu.Unlock()
	case AppendErr_ReqRepeat:
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.myStatus = Follower
			rf.votedFor = -1
			rf.currentTerm = reply.Term
			rf.timer.Reset(rf.voteTimeout)
		}
		rf.mu.Unlock()
	case AppendErr_Commited:
		rf.mu.Lock()

		if args.Term != rf.currentTerm {
			rf.mu.Unlock()
			return false
		}

		rf.nextIndex[server]++

		if reply.Term > rf.currentTerm {
			rf.myStatus = Follower
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.timer.Reset(rf.voteTimeout)
			rf.mu.Unlock()
			return false
		}

		argsNew := &AppendEntriesArgs{
			Term:         args.Term,
			LeaderId:     rf.me,
			PrevLogIndex: 0,
			PreLogTerm:   0,
			Logs:         nil,
			LeaderCommit: args.LeaderCommit,
			LogIndex:     args.LogIndex,
		}
		for rf.nextIndex[server] > 0 {
			argsNew.PrevLogIndex = rf.nextIndex[server] - 1
			if argsNew.PrevLogIndex >= len(rf.logs) {
				rf.nextIndex[server]--
				continue
			}
			argsNew.PreLogTerm = rf.logs[argsNew.PrevLogIndex].Term
			break
		}

		if rf.nextIndex[server] < args.LogIndex+1 {
			argsNew.Logs = rf.logs[rf.nextIndex[server] : args.LogIndex+1]
		}
		reply := new(AppendEntriesReply)
		go rf.sendAppendEntries(server, argsNew, reply, appendNum)
		rf.mu.Unlock()

	case AppendErr_RaftKilled:
		return false
	}
	// DPrintf("%d send AE to %d", rf.me, server)
	return ok
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, votedNum *int) bool {
	if rf.killed() {
		return false
	}

	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	for !ok {
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
		rf.timer.Reset(rf.voteTimeout)
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.votedFor = -1
		}
		rf.mu.Unlock()
	case CandidateLogTooOld:
		rf.mu.Lock()
		rf.myStatus = Follower
		rf.timer.Reset(rf.voteTimeout)
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.votedFor = -1
		}
		rf.mu.Unlock()
	case Nil, VotedThisTerm:
		rf.mu.Lock()
		if reply.VoteGranted && reply.Term == rf.currentTerm && *votedNum <= len(rf.peers)/2 {
			*votedNum++
		}

		if *votedNum > len(rf.peers)/2 {
			*votedNum = 0

			if rf.myStatus == Leader {
				rf.mu.Unlock()
				return ok
			}
			// 投票数过半，当选leader
			rf.myStatus = Leader
			// DPrintf("[%d] become leader in term %d", rf.me, rf.currentTerm)
			rf.nextIndex = make([]int, len(rf.peers))
			// nextIndex 这个数组指的是每一个节点的日志的下一个索引，所以都是为len(rf.logs) ，而当前的最大索引应该是len(rf.logs) - 1
			for i, _ := range rf.nextIndex {
				rf.nextIndex[i] = len(rf.logs)
			}

			rf.timer.Reset(HeartBeatTimeout)

		}
		rf.mu.Unlock()

	case RaftKilled:
		return false
	}

	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
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
	rf.mu.Unlock()
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

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
				votedNum := 1 // 获取到的投票数
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

				appendNum := 1
				rf.timer.Reset(HeartBeatTimeout)
				for i, _ := range rf.peers {
					if i == rf.me { // 自己就不用请求了
						continue
					}
					args := &AppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderId:     rf.me,
						PrevLogIndex: 0,
						PreLogTerm:   0,
						Logs:         nil,
						LeaderCommit: rf.commitIndex,
						LogIndex:     len(rf.logs) - 1,
					}

					// 将 rf.nextIndex 和rf.logs长度同步
					for rf.nextIndex[i] > 0 {
						// args.PrevLogIndex 等于 nextIndex的前一个索引
						args.PrevLogIndex = rf.nextIndex[i] - 1

						if args.PrevLogIndex >= len(rf.logs) {
							rf.nextIndex[i]--
							continue
						}

						args.PreLogTerm = rf.logs[args.PrevLogIndex].Term
						break
					}
					// 确认日志是否有新增
					// DPrintf("args.PrevLogIndex = %d", args.PrevLogIndex)
					// DPrintf("server[%d] , rf.nextIndex[i] = %d , len(rf.logs) = %d", i, rf.nextIndex[i], len(rf.logs))
					if rf.nextIndex[i] < len(rf.logs) {
						args.Logs = rf.logs[rf.nextIndex[i] : args.LogIndex+1]
					}

					reply := new(AppendEntriesReply)

					go rf.sendAppendEntries(i, args, reply, &appendNum)
				}

			}
			rf.mu.Unlock()
		}

	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = nil
	rf.matchIndex = nil
	rf.logs = []logEntries{{0, nil}}
	rf.myStatus = Follower
	rand.Seed(time.Now().UnixNano())
	rf.voteTimeout = time.Duration(rand.Intn(150)+200) * time.Millisecond
	rf.timer = time.NewTicker(rf.voteTimeout)
	rf.applyChan = applyCh
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
