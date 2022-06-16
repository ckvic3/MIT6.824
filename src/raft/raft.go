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

	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
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

//
// a struct to hold information about each log entry.
//
type entry struct {
	Term    int
	Command interface{}
}

const (
	LEADER    = 0
	FOLLOWER  = 1
	CANDIDATE = 2
)

func intToState(state int) string {

	var ret string
	switch state {
	case 0:
		ret = "LEADER"
	case 1:
		ret = "FOLLOWER"
	case 2:
		ret = "CANDIDATE"
	}
	return ret
}

var HEART_BEAT_TIME = time.Millisecond * 100

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	log log.Logger

	// Persistent state on all servers:
	currentTerm int
	votedFor    interface{}
	logs        []*entry
	// Volatile state on all servers:

	// index of highest log entry known to be
	// committed (initialized to 0, increases
	// monotonically)
	commitIndex int

	// index of highest log entry applied to state
	// machine (initialized to 0, increases
	// monotonically)
	lastApplied int

	electionTimer *time.Timer

	heartBeatTimer *time.Timer

	state int
	// Volatile state on leaders:

	// for each server, index of the next log entry
	// to send to that server (initialized to leader
	// last log index + 1)
	nextIndex []int
	// for each server, index of highest log entry
	// known to be replicated on server
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int
	var isleader bool

	// Your code here (2A).
	term = rf.currentTerm

	if rf.state == LEADER {
		isleader = true
	} else {
		isleader = false
	}

	rf.log.Printf("node %v current Term is %v, current state is %v", rf.me, term, intToState(rf.state))

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
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

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
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

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate’s term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry
	LastLogTerm  int // term of candidate’s last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 无论Term 谁大谁小， 最终返回该节点的原始 Term 即可
	// 若args.Term 更大或相等，则reply.Term 并不会产生作用
	// 只有当 args.Term 小于 rf.currentTerm 时， reply.Term 才会产生作用
	reply.Term = rf.currentTerm

	if args.Term > rf.currentTerm {
		// 更新 本节点状态
		rf.currentTerm = args.Term
		rf.changeState(FOLLOWER)
		rf.votedFor = args.CandidateId
		rf.electionTimer.Reset(randomizeTime())

		// 处理 RPC reply
		reply.VoteGranted = true

		rf.log.Printf("node %v vote to node %v \n", rf.me, args.CandidateId)
		return
	}

	// 1、 本节点比请求节点领先
	// 2、 或本节点已经投票给其他节点, 对于Candidate 先来先服务
	if args.Term < rf.currentTerm || (args.Term == rf.currentTerm && rf.votedFor != nil && rf.votedFor != args.CandidateId) {
		reply.VoteGranted = false
		rf.log.Printf("node %v don't vote to node %v, because server has voted or request term smaller than current term \n", rf.me, args.CandidateId)
		return
	}

	// paper 5.4.1 Election Restriction
	// determines which of two logs is more up-to-date
	// by comparing the index and term of the last entries in the
	// logs. If the logs have last entries with different terms, then
	// the log with the later term is more up-to-date. If the logs
	// end with the same term, then whichever log is longer is
	// more up-to-date.

	if args.LastLogTerm < rf.logs[len(rf.logs)-1].Term {
		reply.VoteGranted = false
		return
	} else if args.LastLogTerm > rf.logs[len(rf.logs)-1].Term {
		reply.VoteGranted = true

		rf.votedFor = args.CandidateId
		rf.currentTerm = args.Term
		rf.changeState(FOLLOWER)
		rf.electionTimer.Reset(randomizeTime())
		return
	} else {
		if args.LastLogIndex < len(rf.logs) {
			reply.VoteGranted = false
			return
		} else if args.LastLogIndex >= len(rf.logs) {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			rf.currentTerm = args.Term
			rf.changeState(FOLLOWER)
			rf.electionTimer.Reset(randomizeTime())
			return
		}
	}

	// 剩下的就是二者 Term 相同，但本节点还未投票. 这种情况发生的概率？
	rf.log.Printf("Term equal, but node has't vote! \n")
	reply.VoteGranted = true
	rf.changeState(FOLLOWER)
	rf.electionTimer.Reset(randomizeTime())
	rf.votedFor = args.CandidateId
	return
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogTerm  int
	Entry        entry
	LeaderCommit int // leader’s commitIndex
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.log.Printf("node %v receive heart beat from node %v \n", rf.me, args.LeaderId)

	// 当LEADER 收到 heart beat时
	if rf.state == LEADER {
		// 这种情况发生于，old LEADER 失联后 重新连接。此时会出现两个LEADER
		// 比较二者间log 序列号
		reply.Term = rf.currentTerm
		if rf.currentTerm < args.Term {
			reply.Success = true
			reply.Term = rf.currentTerm
			rf.changeState(FOLLOWER)
			rf.electionTimer.Reset(randomizeTime())
		} else {
			reply.Success = false
		}
		return
	}

	// 节点当前为 follower 或者 candidate 时
	rf.electionTimer.Reset(randomizeTime())
	rf.log.Printf("reset election timer \n")
	rf.changeState(FOLLOWER)

	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	reply.Term = rf.currentTerm
	reply.Success = true
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
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	rf.mu.Lock()
	if rf.state != LEADER {
		isLeader = false
	} else {
		rf.logs = append(rf.logs, &entry{Term: rf.currentTerm, Command: command})
		index = len(rf.logs)

		// 异步操作,并不会阻塞
		rf.sendLogEntries()

		isLeader = true
		term = rf.currentTerm
	}
	rf.mu.Unlock()

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
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
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) election() {

	args := RequestVoteArgs{}
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	if len(rf.logs) == 0 {
		args.LastLogIndex = 0
		args.LastLogTerm = 0
	} else {
		args.LastLogIndex = len(rf.logs) - 1
		args.LastLogTerm = rf.logs[len(rf.logs)-1].Term
	}

	receiveVoteNum := 1

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(index int) {

			reply := RequestVoteReply{}
			if rf.sendRequestVote(index, &args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.VoteGranted {
					receiveVoteNum += 1
					if receiveVoteNum > len(rf.peers)/2 && rf.state == CANDIDATE {
						rf.log.Printf("server %v is LEADER \n", rf.me)
						rf.changeState(LEADER)
						// 获得选举胜利后立即发送 heart beat
						rf.HeartBeat()
						rf.heartBeatTimer.Reset(HEART_BEAT_TIME)
					}
				} else if reply.Term > rf.currentTerm {
					rf.changeState(FOLLOWER)
					rf.currentTerm = reply.Term
					rf.electionTimer.Reset(randomizeTime())
				}
			} else {
				rf.log.Printf("node %v send election request to %v failed \n", rf.me, index)
			}
		}(i)
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {

	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()

			if rf.state != LEADER {
				rf.changeState(CANDIDATE)
				rf.currentTerm += 1
				rf.votedFor = rf.me
				rf.log.Printf("node %v election timeout, begin reElection \n", rf.me)
				rf.election()
			}
			rf.electionTimer.Reset(randomizeTime())
			rf.mu.Unlock()

		case <-rf.heartBeatTimer.C:
			rf.mu.Lock()
			rf.HeartBeat()
			rf.mu.Unlock()

		}

	}
}

func (rf *Raft) HeartBeat() {
	for rf.killed() == false {

		if rf.state == LEADER {

			rf.log.Printf("node %v send heart beat to others \n", rf.me)
			args := AppendEntriesArgs{}
			args.Term = rf.currentTerm
			args.LeaderId = rf.me
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				go func(index int) {
					reply := AppendEntriesReply{}

					if rf.sendAppendEntries(index, &args, &reply) {
						rf.mu.Lock()
						if rf.currentTerm != args.Term {
							rf.log.Printf("node %v Term has changed after send headbeat \n", rf.me)
						}
						if reply.Term > rf.currentTerm {
							rf.currentTerm = reply.Term
							rf.changeState(FOLLOWER)
						}
						rf.mu.Unlock()
					} else {
						rf.log.Printf("node %v send heart beat to node %v failed \n", rf.me, index)
					}

				}(i)

			}
			rf.heartBeatTimer.Reset(HEART_BEAT_TIME)
		}
		return
	}

}

func (rf *Raft) sendLogEntries() {

	for rf.killed() == false {

		for i := 0; i < len(rf.peers); i++ {
			if rf.me == i {
				continue
			} else {
				go func(index int) {

					if len(rf.logs) > rf.nextIndex[index] {

						args := AppendEntriesArgs{Term: rf.currentTerm,
							LeaderId:     rf.me,
							PrevLogTerm:  rf.logs[rf.nextIndex[index]-1].Term,
							Entry:        *rf.logs[rf.nextIndex[index]],
							LeaderCommit: rf.commitIndex}
						reply := AppendEntriesReply{}

						if rf.sendAppendEntries(index, &args, &reply) {

							rf.mu.Lock()
							if reply.Success {
								rf.nextIndex[index] += 1
							} else if reply.Success == false && reply.Term > rf.currentTerm {
								rf.currentTerm = reply.Term
								rf.changeState(FOLLOWER)
								rf.electionTimer.Reset(randomizeTime())
							}
							rf.mu.Unlock()
						}
					}
				}(i)

			}
		}
		rf.heartBeatTimer.Reset(HEART_BEAT_TIME)
		return

	}
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	filename := fmt.Sprintf("./%v.log", rf.me)
	f, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR|os.O_TRUNC, os.ModePerm)
	if err == nil {
		rf.log.SetOutput(f)
		rf.log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile | log.Lmicroseconds)
	}

	rf.logs = make([]*entry, 0)
	rf.currentTerm = 0
	rf.votedFor = nil
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.state = FOLLOWER
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.electionTimer = time.NewTimer(randomizeTime())
	rf.heartBeatTimer = time.NewTimer(HEART_BEAT_TIME)

	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = 0
		rf.matchIndex[i] = 0
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func (rf *Raft) changeState(state int) {

	if rf.state != state {
		rf.log.Printf("node %v state from %v to %v \n", rf.me, rf.state, state)
		rf.state = state

	}

}

func randomizeTime() time.Duration {

	ElectionTimeout := 300 * time.Millisecond
	r := time.Duration(rand.Int63()) % ElectionTimeout

	return r + ElectionTimeout
}
