package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft Peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"log"
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

// import "bytes"
// import "../labgob"

//
// as each Raft Peer becomes aware that successive log entries are
// committed, the Peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// A Go object implementing a single Raft Peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this Peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this Peer's persisted state
	me        int                 // this Peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//任期号
	Term int32
	//获得选票的服务器
	VotedFor int
	//角色类型
	Role int32
	//下一个超时时间
	NextTimeout time.Time
}

func (rf *Raft) heartbeatTimeout() bool {
	//log.Printf("[raft-%v %v %v] now = %v,timeout = %v \n", rf.me, getRole(rf.Role), rf.Term, time.Now(), timeout)
	return rf.NextTimeout.Before(time.Now())
}

func (rf *Raft) isLeader() bool {
	return rf.Role == Leader
}

func (rf *Raft) isFollower() bool {
	return rf.Role == Follower
}

func (rf *Raft) isCandidate() bool {
	return rf.Role == Candidate
}

func (rf *Raft) updateHeartbeatTime() {
	rt := time.Duration(rand.Uint32()%150) + 150
	rf.NextTimeout = time.Now().Add(rt * time.Millisecond)
	//log.Printf("[raft-%v %v %v] 更新心跳时间 = %v \n", rf.me, rf.getRole(), rf.Term, rf.NextTimeout)
}

func (rf *Raft) changeToLeader() {
	log.Printf("[raft-%v %v %v] 修改状态 => Leader .\n", rf.me, rf.getRole(), rf.Term)
	rf.Role = Leader
	//log.Printf("[raft-%v %v] 修改后状态 => %v .\n", rf.me, rf.Term, getRole(rf.Role))
}

func (rf *Raft) changeToCandidate() {
	log.Printf("[raft-%v %v %v] 修改状态 => Candidate.\n", rf.me, rf.getRole(), rf.Term)
	rf.Role = Candidate
}

// 变成follower,重置投票情况
func (rf *Raft) changeToFollower(term int32) {
	log.Printf("[raft-%v %v %v] 修改状态 => Follow. biggerTerm = %v \n", rf.me, rf.getRole(), rf.Term, term)
	rf.Role = Follower
	rf.Term = term
	rf.VotedFor = -1
	rf.updateHeartbeatTime()
}

const Leader int32 = 1
const Follower int32 = 2
const Candidate int32 = 3

func (rf *Raft) getRole() string {
	if rf.Role == Leader {
		return "Leader"
	}
	if rf.Role == Follower {
		return "Follower"
	}
	if rf.Role == Candidate {
		return "Candidate"
	}
	log.Panicf("get Role error, r = %v \n", rf.Role)
	return "Unknown"
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your Code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = int(rf.Term)
	isleader = rf.isLeader()
	//log.Printf("[raft-%v %v %v] get state = %v\n", rf.me, getRole(rf.Role), term, isleader)
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your Code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your Code here (2C).
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

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	// 竞选人id
	Peer int
	// 竞选人term
	Term int32
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Peer        int
	Term        int32
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Peer int
	Term int32
}

type AppendEntriesReply struct {
	Term   int32
	Result bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your Code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() {
		return
	}

	log.Printf("[raft-%v %v %v] 处理%v的投票RPC. T = %v \n", rf.me, rf.getRole(), rf.Term, args.Peer, args.Term)
	reply.Term, reply.VoteGranted = rf.Term, false

	// term太小，不理
	if args.Term < rf.Term {
		return
	}

	// 更大的term
	if args.Term > rf.Term {
		rf.changeToFollower(args.Term)
	}

	//每个任期，只能投票一次
	if rf.VotedFor == -1 || rf.VotedFor == args.Peer {
		//进行投票
		reply.VoteGranted = true
		rf.VotedFor = args.Peer
		rf.updateHeartbeatTime()
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() {
		return
	}
	//log.Printf("[raft-%v %v %v] 接收来自%v的心跳请求. T = %v.\n", rf.me, rf.getRole(), rf.Term, args.Peer, args.Term)
	reply.Term, reply.Result = rf.Term, args.Term >= rf.Term

	if args.Term < rf.Term {
		return
	} else if rf.isCandidate() {
		rf.changeToFollower(args.Term)
	} else {
		rf.updateHeartbeatTime()
	}

	// 日志操作lab-2A不实现
	rf.persist()
}

//
// example Code to send a RequestVote RPC to a server.
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
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
// Term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your Code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your Code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your Code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
// params:
// peers is an array of network identifiers of the Raft peers
// me is the index of this Peer
//
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	//log.Printf("== Make() ==")
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	// Your initialization Code here (2A, 2B, 2C).
	rf.Term = 0
	rf.Role = Follower
	rf.VotedFor = -1
	// 设置下次心跳检查时间
	rf.updateHeartbeatTime()
	// 维持状态的协程
	go rf.maintainStateLoop()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func (rf *Raft) maintainStateLoop() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.isLeader() {
			rf.maintainsLeader()
		} else if rf.isFollower() {
			rf.maintainsFollower()
		} else if rf.isCandidate() {
			rf.maintainsCandidate()
		} else {
			log.Fatalf("[raft-%v %v]Role type error : %v \n", rf.me, rf.getRole(), rf.Role)
		}
	}
}

// leader
// step1 : send heartbeat to peers
func (rf *Raft) maintainsLeader() {
	//log.Printf("[raft-%v %v %v] 发送心跳RPC. now = %v", rf.me, rf.getRole(), rf.Term, time.Now().Local())
	args := AppendEntriesArgs{Peer: rf.me, Term: rf.Term}
	for idx := range rf.peers {
		if idx == rf.me {
			continue
		}
		go func(from int, to int, term int32) {
			reply := AppendEntriesReply{}
			flag := rf.sendAppendEntries(to, &args, &reply)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if !rf.killed() && rf.isLeader() && flag && reply.Term > rf.Term && args.Term == rf.Term {
				rf.changeToFollower(reply.Term)
			}
		}(rf.me, idx, rf.Term)
	}
	rf.mu.Unlock()
	time.Sleep(100 * time.Millisecond)
}

func (rf *Raft) maintainsFollower() {
	if rf.heartbeatTimeout() {
		log.Printf("[raft-%v %v %v] 心跳超时. now = %v", rf.me, rf.getRole(), rf.Term, time.Now().Local())
		rf.changeToCandidate()
		rf.mu.Unlock()
	} else {
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) maintainsCandidate() {
	// 检查超时
	if !rf.heartbeatTimeout() {
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		return
	}

	// 发送投票RPC
	log.Printf("[raft-%v %v %v] == 发起投票RPC == \n", rf.me, rf.getRole(), rf.Term)
	rf.VotedFor = rf.me
	rf.Term += 1
	maxTerm, sumVotes := rf.Term, len(rf.peers)
	voteReply := make(chan *RequestVoteReply)
	args := RequestVoteArgs{Peer: rf.me, Term: rf.Term}
	for idx := range rf.peers {
		if rf.me == idx {
			continue
		}
		go func(from int, to int, term int32) {
			reply := RequestVoteReply{Peer: to, Term: term, VoteGranted: false}
			_ = rf.sendRequestVote(to, &args, &reply)
			voteReply <- &reply
		}(rf.me, idx, rf.Term)
	}
	rf.mu.Unlock()

	// 处理投票回复
	validVotes, acceptVotes := 1, 1
	select {
	case reply := <-voteReply:
		validVotes++
		if reply.VoteGranted {
			acceptVotes++
		} else if reply.Term > maxTerm {
			maxTerm = reply.Term
		}
		if validVotes > sumVotes/2 {
			goto VotedDone
		}
	}

	//汇总投票结果
VotedDone:
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() || !rf.isCandidate() {
	} else if maxTerm > rf.Term {
		log.Printf("[raft-%v-%v-%v] 投票结果: 有更大的Term,放弃竞选: %v.\n", rf.me, rf.getRole(), rf.Term, maxTerm)
		rf.changeToFollower(maxTerm)
	} else if rf.isQuorum(acceptVotes) {
		log.Printf("[raft-%v-%v-%v] == 投票通过: 总票数 = %v. 赞同票数 = %v == \n", rf.me, rf.getRole(), rf.Term, sumVotes, acceptVotes)
		rf.changeToLeader()
	} else {
		log.Printf("[raft-%v-%v-%v] == 投票未通过: 总票数 = %v. 赞同票数 = %v == \n", rf.me, rf.getRole(), rf.Term, sumVotes, acceptVotes)
		rf.updateHeartbeatTime()
	}
}

func (rf *Raft) isQuorum(accept int) bool {
	return accept > len(rf.peers)/2
}
