package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(Command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"fmt"
	"math/rand"

	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"distributed-systems/labgob"
	"distributed-systems/labrpc"
)

// as each Raft peer becomes aware that successive log Entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
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
	currentTerm int // latest Term server has seen (initialized to 0 on first boot, increases monotonically)
	voteFor     int // CandidateId that received vote in current Term (or null if none)
	log         []*LogEntry

	//Volatile state on all servers
	leaderId           int           // who is the leader
	lastTimeFromLeader time.Time     // the last time the follower received an RPC from the leader.
	heartbeatTimeout   time.Duration // period between each heartbeat
	termTimout         time.Duration // period between each election: 3x heartbeatPeriod
	commitIndex        int           // index of highest log entry known to be committed(initialized to 0, increases monotonically)
	lastApplied        int           // index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	// Volatile state on leaders(Reinitialized after election).
	nextIndex  []int //for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int //for each server, index of the highest log entry known to be replicated on server (initialized to 0, increases monotonically)

}

// log Entries; each entry contains Command
// for state machine, and Term when entry
// was received by leader (first index is 1)
type LogEntry struct {
	Command interface{}
	Term    int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = (rf.leaderId == rf.me) && (rf.voteFor == rf.me) // todo 如何避免已经超时的自以为是的leader

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
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
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
	Term         int // candidate’s Term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry (§5.4)
	LastLogTerm  int // Term of candidate’s last log entry (§5.4)
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // 'true' means candidate received vote
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	//Receiver implementation:
	//1. Reply false if Term < currentTerm (§5.1)
	//2. If votedFor is null or CandidateId, and candidate’s log is at
	//least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		fmt.Printf("RequestVote: (%+v).Term < my(%d) currentTerm(%d) \n", args, rf.me, rf.currentTerm)
		return
	}

	if rf.voteFor < 0 || rf.voteFor == args.CandidateId {
		if args.LastLogIndex >= len(rf.log)-1 {
			rf.voteFor = args.CandidateId
			rf.currentTerm = args.Term

			reply.Term = rf.currentTerm
			reply.VoteGranted = true

			fmt.Printf("RequestVote: (%+v).LastLogIndex >= my(%d) last log index(%d) \n", args, rf.me, len(rf.log)-1)
			return
		}
	}
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// AppendEntriesReq for a leader to replicate log to followers; or heartbeat.
type AppendEntriesReq struct {
	Term         int         // leader’s Term
	LeaderId     int         // so follower can redirect clients
	PrevLogIndex int         // index of log entry immediately preceding new ones
	PrevLogTerm  int         // Term of PrevLogIndex entry
	Entries      []*LogEntry // log Entries to store (empty for heartbeat;  may send more than one for efficiency)
	LeaderCommit int         // leader’s commitIndex
}

// AppendEntriesReply
type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching PrevLogIndex and PrevLogTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesReq, reply *AppendEntriesReply) {
	// lock to ensure safety
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//Receiver implementation:
	//1. Reply false if Term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		fmt.Printf("AppendEntries: (%+v).Term < my(%d) currentTerm(%d) \n", args, rf.me, rf.currentTerm)
		return
	}

	//heartbeats
	if len(args.Entries) == 0 {
		rf.lastTimeFromLeader = time.Now()
		rf.leaderId = args.LeaderId
		rf.currentTerm = args.Term

		reply.Success = true
		reply.Term = rf.currentTerm
		fmt.Printf("AppendEntries: heatbeat(%+v) \n", args)
		return
	}

	//2. Reply false if log doesn’t contain an entry at PrevLogIndex
	//whose Term matches PrevLogTerm (§5.3)
	// raft lead know about each follower's nextIndex, so PrevLogIndex won't exceed follower's log boundary.
	//if args.PrevLogIndex < len(rf.log) && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		reply.Term = rf.currentTerm

		// todo
		fmt.Printf("AppendEntries: heatbeat(%+v) \n", args)
		return
	}

	//3. If an existing entry conflicts with a new one (same index
	//but different terms), delete the existing entry and all that
	//follow it (§5.3)
	appendPos := 0
	for i := 0; i < len(args.Entries); i++ {
		logIndex := args.PrevLogIndex + 1 + i
		if logIndex >= len(rf.log) {
			break // out of bounds of the follower's log
		}
		if rf.log[logIndex].Term != args.Entries[i].Term {
			rf.log = rf.log[:logIndex]
			appendPos = i
			break
		}
	}

	//4. Append any new Entries not already in the log
	for i := appendPos; i < len(args.Entries); i++ {
		rf.log = append(rf.log, args.Entries[i])
	}

	//5. If LeaderCommit > commitIndex, set commitIndex =
	//	min(LeaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = Min(args.LeaderCommit, len(rf.log))
	}
	rf.currentTerm = args.Term

	reply.Success = true
	reply.Term = rf.currentTerm
	return
}

func Min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesReq, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next Command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// Command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the Command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

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
		now := time.Now()
		lastTime := rf.lastTimeFromLeader // copy to avoid concurrency problem
		if now.After(lastTime) {
			// Send requestVote RPC asynchronously and wait majority to grant.
			// To make sure a leader will be elected finally, keep trying
			// until other peer has been elected as the leader: stop election, turn myself candidate -> follower;
			// or other peers has granted me as the leader: stop election, turn myself candidate -> leader.
			if now.Sub(lastTime).Milliseconds() > rf.termTimout.Milliseconds() {
				rf.tryElection(lastTime)

				//wait for the next timeout
				timer := time.NewTimer(rf.lastTimeFromLeader.Add(rf.heartbeatTimeout).Sub(time.Now()))
				<-timer.C
			} else {
				// todo heart beat
			}
		} else {
			// clock is moved back
			fmt.Printf("[warning] the clock is moved back, last:%v, now:%v \n", rf.lastTimeFromLeader, now)
		}
	}
}

func (rf *Raft) tryElection(lastTime time.Time) {
	// sleep in random time to minimize split vote problem
	randSleep := rand.Intn(int(rf.heartbeatTimeout.Milliseconds()))
	time.Sleep(time.Millisecond * time.Duration(randSleep))

	halfPeers := int32(len(rf.peers) / 2)
	votesGranted := int32(1) // vote for myself
	rf.voteFor = rf.me
	for i, _ := range rf.peers {
		peerNo := i
		if peerNo == rf.me {
			continue
		}

		go func() {
			if rf.killed() == false {
				args := &RequestVoteArgs{
					Term:        rf.currentTerm,
					CandidateId: rf.me,
				}
				if len(rf.log) > 0 {
					args.LastLogIndex = len(rf.log) - 1
					args.LastLogTerm = rf.log[len(rf.log)-1].Term
				}
				reply := &RequestVoteReply{}
				rf.sendRequestVote(peerNo, args, reply)
				if reply.VoteGranted {
					atomic.AddInt32(&votesGranted, 1)
				}
			}
		}()
	}

	// wait to see votes (don't use waitGroup, it's blocking)
	timer := time.NewTimer(rf.heartbeatTimeout)
	defer timer.Stop()
	currentTime := <-timer.C
	if atomic.LoadInt32(&votesGranted) > halfPeers {
		rf.turnToLeader(currentTime)
	}

	// if timeout, and I were not elected as the leader,
	// return to see if any peer became a leader, so that my lastTimeFromLeader is updated.
	return
}

func (rf *Raft) turnToLeader(currentTime time.Time) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// if I were elected as the leader, lock myself, update timeout.
	// todo call appendEntries to all the other peers, then update my nextIndex and matchIndex
	rf.lastTimeFromLeader = currentTime
	rf.leaderId = rf.me
	req := &AppendEntriesReq{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      nil, // this is empty for heartbeat
		LeaderCommit: rf.commitIndex,
	}

	reply := &AppendEntriesReply{
		Term:    0,
		Success: false,
	}

	for i, _ := range rf.peers {
		rf.sendAppendEntries(i, req, reply)
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
	rf.voteFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.lastTimeFromLeader = time.Unix(0, 0)
	rf.heartbeatTimeout = 100 * time.Millisecond
	rf.termTimout = 3 * rf.heartbeatTimeout
	rf.lastTimeFromLeader = time.UnixMilli(0)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
