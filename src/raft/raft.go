package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, CurTerm, isleader)
//   start agreement on a new log entry
// rf.GetState() (CurTerm, isLeader)
//   ask a Raft for its current CurTerm, and whether it thinks it is leader
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
	CurTerm int32
	//获得选票的服务器
	VotedFor int32
	//角色类型
	Role int32
	//下一次心跳的时间
	NextHeartbeatTime time.Time
	NextVoteTime      time.Time
}

func (rf *Raft) updateHeartbeatTime() {
	dur := time.Duration((rand.Int() % 300) + 300)
	rf.NextHeartbeatTime = time.Now().Add(dur * time.Millisecond)
	//log.Printf("[raft-%v] 更新心跳时间 = %v \n", rf.me, rf.NextHeartbeatTime)
}

func (rf *Raft) updateVoteTime() {
	dur := time.Duration((rand.Int() % 300) + 100)
	rf.NextVoteTime = time.Now().Add(dur * time.Millisecond)
}

func (rf *Raft) changeToLeader() {
	log.Printf("[raft-%v] 修改状态 => Leader.\n", rf.me)
	rf.Role = Leader
}

func (rf *Raft) changeToCandidate() {
	log.Printf("[raft-%v] 修改状态 => Candidate.\n", rf.me)
	rf.Role = Candidate
	rf.NextVoteTime = time.Now()
}

func (rf *Raft) changeToFollower(term int32) {
	rf.Role = Follower
	rf.CurTerm = term
	log.Printf("[raft-%v] 修改状态 => Follower. term = %v \n", rf.me, rf.CurTerm)
	rf.updateHeartbeatTime()
}

func (rf *Raft) Peers() []*labrpc.ClientEnd {
	return rf.peers
}

//func (rf *Raft) SleepTime() time.Duration {
//	return rf.RandomSleepTime
//}

const Leader int32 = 1
const Follower int32 = 2
const Candidate int32 = 3

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your Code here (2A).
	rf.mu.Lock()
	term = int(rf.CurTerm)
	isleader = rf.Role == Leader
	rf.mu.Unlock()
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
	Term        int32
	VoteGranted bool
}

type AppendEntriesArgs struct {
	LeaderPeer int
	LeaderTerm int32
}

type AppendEntriesReply struct {
	FollowerTerm int32
	AppendResult bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your Code here (2A, 2B).
	rf.mu.Lock()
	log.Printf("[raft-%v] 处理来自%v的投票. self.term = %v. req.term = %v \n", rf.me, args.Peer, rf.CurTerm, args.Term)
	reply.VoteGranted = rf.CurTerm <= args.Term
	reply.Term = rf.CurTerm
	if args.Term > rf.CurTerm {
		rf.changeToFollower(args.Term)
	}
	rf.mu.Unlock()
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//log.Printf("[raft-%v] 接收来自%v的心跳请求. Term = %v. self.term = %v \n", rf.me, args.LeaderPeer, args.LeaderTerm, rf.CurTerm)
	rf.mu.Lock()
	reply.FollowerTerm = rf.CurTerm
	reply.AppendResult = args.LeaderTerm >= rf.CurTerm
	if rf.CurTerm < args.LeaderTerm {
		rf.changeToFollower(args.LeaderTerm)
	} else {
		rf.updateHeartbeatTime()
	}
	rf.mu.Unlock()
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
// CurTerm. the third return value is true if this server believes it is
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	// Your initialization Code here (2A, 2B, 2C).
	rf.CurTerm = 0
	rf.Role = Follower
	// 设置投票时间
	rf.NextVoteTime = time.Now()
	// 设置下次心跳检查时间
	rf.updateHeartbeatTime()
	// 启动协程负责状态维持
	go rf.maintainStateLoop()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func (rf *Raft) maintainStateLoop() {
	for {
		rf.mu.Lock()
		role := rf.Role
		rf.mu.Unlock()

		if role == Leader {
			rf.maintainsLeader()
		} else if role == Follower {
			rf.maintainsFollower()
		} else if role == Candidate {
			rf.maintainsCandidate()
		} else {
			log.Fatalf("[raft-%v]Role type error : %v \n", rf.me, rf.Role)
		}
	}
}

// leader
// step1 : send heartbeat to peers
func (rf *Raft) maintainsLeader() {
	//log.Printf("[raft-%v] maintains leader \n", rf.me)
	rf.mu.Lock()
	wg := sync.WaitGroup{}
	wg.Add(len(rf.peers) - 1)
	for idx := range rf.peers {
		if idx == rf.me {
			continue
		}
		go func(from int, to int, term int32) {
			args := AppendEntriesArgs{LeaderPeer: from, LeaderTerm: term}
			reply := AppendEntriesReply{}
			//log.Printf("[raft-%v] 发送心跳给%v \n", from, to)
			flag := rf.sendAppendEntries(to, &args, &reply)
			rf.mu.Lock()
			if flag {
				if reply.FollowerTerm > term {
					rf.changeToFollower(reply.FollowerTerm)
				}
			} else {

			}
			rf.mu.Unlock()
			wg.Done()
		}(rf.me, idx, rf.CurTerm)
	}
	rf.mu.Unlock()
	wg.Wait()
	time.Sleep(10)
}

func (rf *Raft) maintainsFollower() {
	rf.mu.Lock()
	now := time.Now()
	hb := rf.NextHeartbeatTime
	if now.Before(hb) {
		time.Sleep(10 * time.Millisecond)
	} else {
		log.Printf("[raft-%v] 心跳超时. now = %v, hb = %v  \n", rf.me, now, hb)
		rf.changeToCandidate()
	}
	rf.mu.Unlock()

}

func (rf *Raft) maintainsCandidate() {
	if time.Now().Before(rf.NextVoteTime) {
		time.Sleep(10)
		return
	}
	log.Printf("[raft-%v] == 准备投票 == \n", rf.me)
	rf.mu.Lock()
	sumVotes := len(rf.Peers())
	acceptVotes := 1
	wg := sync.WaitGroup{}
	rf.CurTerm = rf.CurTerm + 1
	wg.Add(sumVotes - 1)
	// send vote to peers
	for idx := range rf.peers {
		if rf.me == idx {
			continue
		}
		go func(from int, to int, term int32) {
			defer wg.Done()
			args := RequestVoteArgs{Peer: from, Term: term}
			reply := RequestVoteReply{}

			voteRes := rf.sendRequestVote(to, &args, &reply)
			log.Printf("[raft-%v] 来自%v的投票结果: %v. \n", from, to, voteRes)

			rf.mu.Lock()
			if voteRes {
				if reply.VoteGranted {
					acceptVotes++
				} else if reply.Term > term {
					rf.changeToFollower(reply.Term)
				}
			}
			rf.mu.Unlock()
		}(rf.me, idx, rf.CurTerm)
	}
	rf.mu.Unlock()

	// wait for peer's vote resp
	wg.Wait()
	rf.mu.Lock()
	log.Printf("[raft-%v] 投票结果. 总票数 = %v. 赞同票数 = %v. \n", rf.me, sumVotes, acceptVotes)
	if acceptVotes > (sumVotes / 2) {
		rf.changeToLeader()
	} else {
		rf.updateVoteTime()
	}
	rf.mu.Unlock()
}
