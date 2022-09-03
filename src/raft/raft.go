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
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

func min(a, b int) int {
	if a < b {
			return a
	}
	return b
}

type logTopic string
const (
	dError   logTopic = "ERRO"
	dInfo    logTopic = "INFO"
	dLeader  logTopic = "LEAD"
	dTerm    logTopic = "TERM"
	dTimer   logTopic = "TIMR"
	dVote    logTopic = "VOTE"
	dWarn    logTopic = "WARN"
	dRoleChange logTopic = "ROLE"
	dLog logTopic = "LOG1"
)

// Retrieve the verbosity level from an environment variable
func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

var debugStart time.Time
var debugVerbosity int

func init() {
	debugVerbosity = getVerbosity()
	debugStart = time.Now()

	// TODO: Try comment out this line and see what the log looks like.
	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func PrettyLog(topic logTopic, id int, format string, a ...interface{}) {
	if debugVerbosity >= 1 {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v S%d ", time, string(topic), id)
		format = prefix + format
		log.Printf(format, a...)
	}
}

// No more than ten times per second.
const heartbeatInterval = 110 * time.Millisecond
// Election timeout (Min + rand.Intn(Range)) milliseconds.
const elecTimeoutRange = 200
const elecTimeoutMin = 200

func randomElectionTimeout() time.Duration {
	return (time.Duration)(rand.Intn(elecTimeoutRange) + elecTimeoutMin) * time.Millisecond
}


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

type role string

const (
	CANDIDATE role = "CANDIDATE"
	LEADER role = "LEADER"
	FOLLOWER role = "FOLLOWER"
)

// Lab 2B:
// Implement Start()
// Add log entries in AppendEntries
// TODO: Send each newly committed entry on applyCh on each peer.

type LogEntry struct {
	Term int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	n int
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	currentTerm int
	votedFor int
	role role
	lastHeartbeatOrElection time.Time

	logEntries []LogEntry
	commitIndex int
	lastApplied int

	nextIndex []int
	matchIndex []int
	// last log index. len(logEntries) might not be accurate.
	lastIndex int
	applyCh chan ApplyMsg

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (r *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	r.mu.Lock()
	defer r.mu.Unlock()
	term = r.currentTerm
	isleader = (r.role == LEADER)
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (r *Raft) persist() {
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
func (r *Raft) readPersist(data []byte) {
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
func (r *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (r *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

type AppendEntriesArgs struct {
	Term int
	LeaderID int

	LogEntries []LogEntry
	PrevLogIndex int
	PrevLogTerm int
	// leader's commitIndex
	LeaderCommitIndex int
}

type AppendEntriesReply struct {
	Term int
	Success bool
	// Term in the conflict entry. -1 if none.
	XTerm int
	// Index of the first entry with that term. -1 if none.
	XIndex int
	// Follower's log length.
	XLen int
}

// Caller must hold the lock.
func (r *Raft) addEntry(entry LogEntry) {
	ind := r.lastIndex + 1
	if len(r.logEntries) <= ind {
		r.logEntries = append(r.logEntries, entry)
	} else {
		r.logEntries[ind] = entry
	}
	r.lastIndex++
}

func (r *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	r.mu.Lock()
	defer r.mu.Unlock()
	PrettyLog(dTimer, r.me, "at T%d received heartbeat (S%d, %v, T%d)", r.currentTerm, args.LeaderID, args.LogEntries, args.Term)
	// Potentially reset the heartbeat timer but it should be ok.
	// Only reset the timer for current leader.
	if args.Term >= r.currentTerm {
		r.lastHeartbeatOrElection = time.Now()
	}

	reply.Term = r.currentTerm

	if (args.Term < r.currentTerm || r.lastIndex < args.PrevLogIndex || r.logEntries[args.PrevLogIndex].Term != args.PrevLogTerm) {
		reply.Success = false
		reply.XTerm = -1
		reply.XIndex = -1
		reply.XLen = r.lastIndex
		if r.lastIndex >= args.PrevLogIndex && r.logEntries[args.PrevLogIndex].Term != args.PrevLogTerm {
			reply.XTerm = r.logEntries[args.PrevLogIndex].Term
			ind := args.PrevLogIndex
			for ind > 1 && r.logEntries[ind - 1].Term == reply.XTerm {
				ind--
			}
			reply.XIndex = ind
		}
		return
	}
	reply.Success = true
	if args.Term > r.currentTerm || (args.Term == r.currentTerm && r.role == CANDIDATE) {
		PrettyLog(dInfo, r.me, "Got term (%d >= %d), %v converting to follower", args.Term, r.currentTerm, r.role)
		r.convertToFollower(args.Term)
		r.lastHeartbeatOrElection = time.Now()
	}
	// copy leader's entries to the log.
	for i := 1; i <= len(args.LogEntries); i++ {
		ind := args.PrevLogIndex + i
		if len(r.logEntries) <= ind {
			r.logEntries = append(r.logEntries, args.LogEntries[i - 1])
		} else {
			r.logEntries[ind] = args.LogEntries[i - 1]
		}
	}
	r.lastIndex = args.PrevLogIndex + len(args.LogEntries)
	if args.LeaderCommitIndex > r.commitIndex {
		// is it possible that leaderCommit is larger than r.lastIndex??
		r.commitIndex = min(args.LeaderCommitIndex, r.lastIndex)

		if r.commitIndex > r.lastApplied {
			PrettyLog(dLog, r.me, "committed > applied (%d>%d), sending ApplyMsgs", r.commitIndex, r.lastApplied)
			r.sendApplyMessages()
		}
	}
}

func (r *Raft) sendAppendEntries(server int, req *AppendEntriesArgs) {
	resp := AppendEntriesReply{}
	ok := r.peers[server].Call("Raft.AppendEntries", req, &resp)
	// When does this happen?
	if !ok {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if resp.Term > r.currentTerm {
		PrettyLog(dRoleChange, r.me, "Got higher term (%d > %d) from heartbeat responses, converting to follower", resp.Term, r.currentTerm)
		r.convertToFollower(resp.Term)
		r.lastHeartbeatOrElection = time.Now()
		return
	}
	// Unsuccessful because of log inconsistency
	if !resp.Success {
		// r.nextIndex[server]--
		// Follower's log is too short
		if resp.XTerm == -1 {
			r.nextIndex[server] = resp.XLen + 1
		} else {
			// If Leader doesn't have XTerm
			r.nextIndex[server] = resp.XIndex
			// Try find the last log entry for XTerm.
			for i := r.nextIndex[server] - 1; i > 1; i-- {
				if r.logEntries[i - 1].Term == resp.XTerm {
					r.nextIndex[server] = i
					break
				}
			}
		}
		return
	}
	// TODO: Add a check that checks the req contains actual logs.
	// Entries applied successfully by peer.
	r.matchIndex[server] = req.PrevLogIndex + len(req.LogEntries)
	r.nextIndex[server] = r.matchIndex[server] + 1
	PrettyLog(dLog, r.me, "updating S%d matchInd: %d, nextInd: %d on success", server, r.matchIndex[server], r.nextIndex[server])

	// Try to commit entries.
	for i := r.lastIndex; i > r.commitIndex; i-- {
		count := 1
		for k := 0; k < r.n; k++ {
			if k == r.me {
				continue
			}
			PrettyLog(dLeader, r.me, "log ind %d, server %d, matchInd %d", i, k, r.matchIndex[k])
			if r.matchIndex[k] >= i {
				count++
				if count > r.n / 2 {
					r.commitIndex = i
					goto FINISH
				}
			}
		}
	}
FINISH:
	if r.commitIndex > r.lastApplied {
		PrettyLog(dLog, r.me, "committed > applied (%d>%d), sending ApplyMsgs", r.commitIndex, r.lastApplied)
		r.sendApplyMessages()
	}
}

// Caller must hold the r.mu lock.
func (r *Raft) sendApplyMessages() {
	for i := r.lastApplied + 1; i <= r.commitIndex; i++ {
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command: r.logEntries[i].Command,
			CommandIndex: i,
		}
		r.applyCh <- applyMsg
	}
	r.lastApplied = r.commitIndex
}

// This function should return fast.
func (r *Raft) broadcastHeartbeat() {
	// r.mu.Lock()
	// if r.role != LEADER {
	// 	 r.mu.Unlock()
	// 	 return
	// }
	// req := &AppendEntriesArgs{
	// 	Term: r.currentTerm,
	// 	LeaderID: r.me,
	// 	Entries: nil,
	// }
	// curTerm := r.currentTerm
	// r.mu.Unlock()
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.role != LEADER {
		return
	}
	for i := range r.peers {
		i := i
		if i == r.me {
			continue
		}
		var logsToSend []LogEntry
		PrettyLog(dLog, r.me, "preparing log entries nextInd[%d]: %d, lastInd: %d", i, r.nextIndex[i], r.lastIndex)
		if r.nextIndex[i] <= r.lastIndex {
			// copy(logsToSend, r.logEntries[r.nextIndex[i]:r.lastIndex + 1])
			logsToSend = append(logsToSend, r.logEntries[r.nextIndex[i] : r.lastIndex + 1]...)
			PrettyLog(dLog, r.me, "logsToSend: %v", logsToSend)
		}
		req := AppendEntriesArgs{
			Term:	r.currentTerm,
			LeaderID: r.me,
			PrevLogIndex: r.nextIndex[i] - 1,
			PrevLogTerm: r.logEntries[r.nextIndex[i] - 1].Term,
			LeaderCommitIndex: r.commitIndex,
			LogEntries: logsToSend,
		}
		PrettyLog(dLeader, r.me, "Leader sending heartbeat to S%d at T%d", i, r.currentTerm)
		go r.sendAppendEntries(i, &req)
		// go func() {
		// 	PrettyLog(dLeader, r.me, "Leader sending heartbeat to S%d at T%d", i, curTerm)
		// 	r.sendAppendEntries(i, req, resp)

		// 	r.mu.Lock()
		// 	if resp.Term > r.currentTerm {
		// 		PrettyLog(dRoleChange, r.me, "Got higher term (%d > %d) from heartbeat responses, converting to follower", resp.Term, r.currentTerm)
		// 		r.convertToFollower(resp.Term)
		// 	}
		// 	r.mu.Unlock()
		// }()
	}
}

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateID int

	// For election restriction.
	LastIndex int
	LastTerm int
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}

func (r *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	r.mu.Lock()
	defer r.mu.Unlock()
	PrettyLog(dVote, r.me, "Got vote request: (S%d I%d T%d) at T%d", args.CandidateID, args.LastIndex, args.LastTerm, r.currentTerm)

	reply.Term = r.currentTerm

	if args.Term < r.currentTerm {
		return
	}
	if args.Term > r.currentTerm {
		PrettyLog(dVote, r.me, "Candidate term is higher, updating (%d > %d) and converting %v to follower", args.Term, r.currentTerm, r.role)
		r.convertToFollower(args.Term)
	}
	curLastTerm := r.logEntries[r.lastIndex].Term
	// Election restriction.
	if r.role == FOLLOWER && (r.votedFor == -1 || r.votedFor == args.CandidateID) && (args.LastTerm > curLastTerm || (args.LastTerm == curLastTerm && args.LastIndex >= r.lastIndex)) {	// Not sure why r.votedFor == args.CandidateID is needed.
		PrettyLog(dVote, r.me, "granted vote to S%d at T%d", args.CandidateID, r.currentTerm)
		reply.VoteGranted = true
		r.votedFor = args.CandidateID
		r.lastHeartbeatOrElection = time.Now()
	}
}

// func (r *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
// 	ok := r.peers[server].Call("Raft.AppendEntries", args, reply)
// 	return ok
// }

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
func (r *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := r.peers[server].Call("Raft.RequestVote", args, reply)
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
func (r *Raft) Start(command interface{}) (int, int, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.role != LEADER {
		PrettyLog(dLog, r.me, "got cmd(%v) as %v", command, r.role)
		return -1, -1, false
	}
	PrettyLog(dLeader, r.me, "got cmd(%v) as %v", command, r.role)
	r.addEntry(LogEntry{Term: r.currentTerm, Command: command})
	index := r.lastIndex
	term := r.currentTerm
	isLeader := true
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
func (r *Raft) Kill() {
	atomic.StoreInt32(&r.dead, 1)
	// Your code here, if desired.
}

func (r *Raft) killed() bool {
	z := atomic.LoadInt32(&r.dead)
	return z == 1
}

// This method doesn't return fast.
func (r *Raft) startElection() {
	r.mu.Lock()

	// Update relevant states.
	r.currentTerm++
	r.role = CANDIDATE
	r.votedFor = r.me

	req := &RequestVoteArgs{
		Term: r.currentTerm,
		CandidateID: r.me,
		LastIndex: r.lastIndex,
		LastTerm: r.logEntries[r.lastIndex].Term,
	}
	curTerm := r.currentTerm
	r.mu.Unlock()

	var voteMu sync.Mutex
	cond := sync.NewCond(&voteMu)
	votes := 0
	finished := 0

	// Loop over peers and send vote requests.
	for i := range r.peers {
		i := i
		if i == r.me {
			continue
		}
		resp := &RequestVoteReply{}
		go func() {
			PrettyLog(dVote, r.me, "Candidate requesting vote to S%d at T%d", i, curTerm)
			ok := r.sendRequestVote(i, req, resp)

			r.mu.Lock()
			if resp.Term > r.currentTerm {
				PrettyLog(dRoleChange, r.me, "%v got higher term(%d > %d) from S%d, converting to follower", r.role, resp.Term, r.currentTerm, i)
				r.convertToFollower(resp.Term)
			}
			r.mu.Unlock()

			voteMu.Lock()
			defer voteMu.Unlock()
			if ok && resp.VoteGranted {
				votes++
			}
			finished++
			cond.Broadcast()
		}()
	}
	n := len(r.peers)

	voteMu.Lock()
	for votes < (n - 1) / 2 && finished != n - 1 {
		cond.Wait()
	}
	if votes >= (n - 1) / 2 {
		r.mu.Lock()
		if r.currentTerm == curTerm && r.role == CANDIDATE {
			PrettyLog(dVote, r.me, "Candidate got %d votes at T%d, becoming leader", votes, r.currentTerm)
			// r.role = LEADER
			r.convertToLeader()
		}
		r.mu.Unlock()
	}
	voteMu.Unlock()
}

func (r *Raft) convertToLeader() {
	r.role = LEADER
	for i := 0; i < r.n; i++ {
		r.nextIndex[i] = r.lastIndex + 1
		r.matchIndex[i] = 0
	}
}

// Caller must hold r.mu.
func (r *Raft) convertToFollower(term int) {
	r.currentTerm = term
	r.role = FOLLOWER
	r.votedFor = -1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (r *Raft) ticker() {
	electionTimeout := randomElectionTimeout()
	for !r.killed() {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		r.mu.Lock()
		if time.Since(r.lastHeartbeatOrElection) > electionTimeout && (r.role == FOLLOWER || r.role == CANDIDATE){
			PrettyLog(dTimer, r.me, "%v election timeout, starting election", r.role)
			go r.startElection()
			electionTimeout = randomElectionTimeout()
			// Reset the election timer when starting an election.
			r.lastHeartbeatOrElection = time.Now()
		}
		r.mu.Unlock()
		time.Sleep(20 * time.Millisecond)
	}
}

func (r *Raft) leader() {
	for !r.killed() {
		r.mu.Lock()
		role := r.role
		r.mu.Unlock()
		if role == LEADER {
			PrettyLog(dTimer, r.me, "Leader broadcasting heartbeat")
			r.broadcastHeartbeat()
		}
		time.Sleep(heartbeatInterval)
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
	r := &Raft{}
	r.n = len(peers)
	r.peers = peers
	r.persister = persister
	r.me = me

	// Your initialization code here (2A, 2B, 2C).
	r.currentTerm = 0
	r.votedFor = -1
	r.role = FOLLOWER
	r.lastHeartbeatOrElection = time.Now()

	// Add a dummy log so that log index starts at 1.
	r.logEntries = append(r.logEntries, LogEntry{Term: 0})
	r.commitIndex = 0
	r.lastApplied = 0
	r.nextIndex = make([]int, r.n)
	r.matchIndex = make([]int, r.n)
	r.lastIndex = 0
	r.applyCh = applyCh

	// initialize from state persisted before a crash
	r.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go r.ticker()
	go r.leader()

	return r
}
