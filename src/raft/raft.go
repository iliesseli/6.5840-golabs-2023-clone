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
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
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

const (
	ServerFollower  = 0
	ServerCandidate = 1
	ServerLeader    = 2
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	cond      *sync.Cond

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent
	CurrentTerm int
	VotedFor    *int
	LogEntries  []*LogEntry
	// volatile
	CommitIndex int
	LastApplied int
	// volatile leaders
	NextIndex  []int
	MatchIndex []int

	ServerRole        int
	LastHeartBeatTime time.Time
	ElectionTimeout   time.Duration
	VoteCount         int
	ApplyCh           chan ApplyMsg
}

type LogEntry struct {
	Cmd   interface{} `json:"-"`
	Term  int
	Index int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.CurrentTerm, rf.ServerRole == ServerLeader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
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
	// Your data here (2A, 2B). invoke by candidates
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.CurrentTerm {
		rf.handleReplyOrArgsLargerTerm(args.Term)
	}
	reply.Term = rf.CurrentTerm
	if rf.VotedFor == nil || *rf.VotedFor == args.CandidateId {
		lastLogIdx, lastLogTerm := rf.getLastLogIdxAndTerm()
		if args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIdx) {
			reply.VoteGranted = true
			rf.VotedFor = &args.CandidateId
			rf.LastHeartBeatTime = time.Now()
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

func (rf *Raft) callRequestVote(server int, term int, lastlogidx int, lastlogterm int) bool {
	args := RequestVoteArgs{
		Term:         term,
		CandidateId:  rf.me,
		LastLogIndex: lastlogidx,
		LastLogTerm:  lastlogterm,
	}
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(server, &args, &reply)
	if !ok {
		return false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// *** to avoid term confusion !!! ***
	// compare the current term with the term you sent in your original RPC.
	// If the two are different, drop the reply and return
	if term != rf.CurrentTerm {
		return false
	}

	// other server has higher term !
	if reply.Term > rf.CurrentTerm {
		rf.handleReplyOrArgsLargerTerm(reply.Term)
	}
	return reply.VoteGranted
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []*LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.CurrentTerm
	if args.Term < rf.CurrentTerm {
		reply.Success = false
		return
	}
	if args.Term > rf.CurrentTerm {
		rf.handleReplyOrArgsLargerTerm(args.Term)
	}

	rf.LastHeartBeatTime = time.Now()

	// check prevLog
	if len(rf.LogEntries) <= args.PrevLogIndex || rf.LogEntries[args.PrevLogIndex].Term != args.PrevLogTerm {
		DPrintf("Server[%d] check prevLogEntry failed", rf.me)
		reply.Success = false
		return
	}

	// delete conflicting entries and append new entries
	i := 0
	j := args.PrevLogIndex + 1
	DPrintf("%d", j)
	for i = 0; i < len(args.Entries); i++ {
		if j >= len(rf.LogEntries) {
			break
		}
		if rf.LogEntries[j].Term == args.Entries[i].Term {
			j++
		} else {
			rf.LogEntries = append(rf.LogEntries[:j], args.Entries[i:]...)
			i = len(args.Entries)
			j = len(rf.LogEntries) - 1
			break
		}
	}
	if i < len(args.Entries) {
		rf.LogEntries = append(rf.LogEntries, args.Entries[i:]...)
		j = len(rf.LogEntries) - 1
	} else {
		j--
	}
	// DPrintf("Server[%d] LogEntries=%s", rf.me, Marshal(rf.LogEntries))
	// update commit index
	if args.LeaderCommit > rf.CommitIndex {
		originCommitIdx := rf.CommitIndex
		rf.CommitIndex = Min(args.LeaderCommit, j)
		if rf.CommitIndex > originCommitIdx {
			rf.cond.Broadcast()
		}
		// DPrintf("Server[%d] CommitIndex=%d", rf.me, rf.CommitIndex)
	}
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) callAppendEntries(server int, term int, prevLogIndex int, prevLogTerm int, entries []*LogEntry, leaderCommit int) bool {
	args := AppendEntriesArgs{
		Term:         term,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: leaderCommit,
	}
	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(server, &args, &reply)
	if !ok {
		return false
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// *** to avoid term confusion !!! ***
	// compare the current term with the term you sent in your original RPC.
	// If the two are different, drop the reply and return
	if term != rf.CurrentTerm {
		return false
	}

	// other server has higher term !
	if reply.Term > rf.CurrentTerm {
		rf.handleReplyOrArgsLargerTerm(reply.Term)
	}
	return reply.Success

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
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.ServerRole != ServerLeader || rf.killed() {
		return -1, -1, false
	}
	entry := &LogEntry{
		Cmd:   command,
		Term:  rf.CurrentTerm,
		Index: len(rf.LogEntries),
	}
	// prevLogIdx, prevLogTerm := rf.getPrevLogIdxAndTerm(entry.Index)
	rf.LogEntries = append(rf.LogEntries, entry)
	// rf.parallelAppendEntries([]*LogEntry{entry}, prevLogIdx, prevLogTerm)

	return entry.Index, entry.Term, true
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

func (rf *Raft) checkElection() {
	for !rf.killed() {

		// Your code here (2A)
		// Check if a leader election should be started.
		wg := sync.WaitGroup{}
		rf.mu.Lock()
		if rf.ServerRole != ServerLeader && time.Since(rf.LastHeartBeatTime) > rf.ElectionTimeout {
			rf.ServerRole = ServerCandidate
			rf.CurrentTerm += 1
			// vote self
			rf.VoteCount = 1
			rf.VotedFor = &rf.me
			DPrintf("Server[%d] start election currentTerm=%d", rf.me, rf.CurrentTerm)
			for i := 0; i < len(rf.peers); i++ {
				// send voteeq except me
				if i != rf.me {
					wg.Add(1)
					go func(idx int) {
						defer wg.Done()
						rf.mu.Lock()
						if rf.ServerRole != ServerCandidate {
							rf.mu.Unlock()
							return
						}
						lastLogIdx, lastLogTerm := rf.getLastLogIdxAndTerm()
						term := rf.CurrentTerm
						rf.mu.Unlock()
						granted := rf.callRequestVote(idx, term, lastLogIdx, lastLogTerm)
						if granted {
							rf.mu.Lock()
							rf.VoteCount++
							DPrintf("Server[%d] get %d vote", rf.me, rf.VoteCount)
							if rf.VoteCount > len(rf.peers)/2 && rf.ServerRole != ServerLeader {
								DPrintf("Server[%d] become leader", rf.me)
								rf.ServerRole = ServerLeader
								// initalize leader state
								for i := range rf.peers {
									rf.NextIndex[i] = rf.getLastLogIdx() + 1
									rf.MatchIndex[i] = 0
								}
								rf.mu.Unlock()
								// leader send heartbeat
								go rf.leaderHeartbeat()
								go rf.checkCommit()
								rf.startCheckAppend()
							} else {
								rf.mu.Unlock()
							}
						} else {
							// DPrintf("Server[%d] sendRequestVote failed", rf.me)
						}
					}(i)
				}
			}
		}
		rf.mu.Unlock()

		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) broadcastHeartbeat() {
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me && rf.ServerRole == ServerLeader {
			go func(idx int) {
				rf.mu.Lock()
				prevLogIdx, prevLogTerm := rf.getLastLogIdxAndTerm()
				term := rf.CurrentTerm
				entries := make([]*LogEntry, 0)
				leaderCommit := rf.CommitIndex
				rf.mu.Unlock()
				rf.callAppendEntries(idx, term, prevLogIdx, prevLogTerm, entries, leaderCommit)
			}(i)
		}
	}
}

func (rf *Raft) getLastLogIdx() int {
	return len(rf.LogEntries) - 1
}

func (rf *Raft) getPrevLogIdxAndTerm(currentIdx int) (int, int) {
	prevIdx := currentIdx - 1
	prevIdxTerm := rf.LogEntries[prevIdx].Term
	return prevIdx, prevIdxTerm
}

func (rf *Raft) getLastLogIdxAndTerm() (int, int) {
	lastIdx := len(rf.LogEntries) - 1
	lastTerm := rf.LogEntries[lastIdx].Term
	return lastIdx, lastTerm
}

func (rf *Raft) handleReplyOrArgsLargerTerm(term int) {
	rf.CurrentTerm = term
	rf.ServerRole = ServerFollower
	rf.VotedFor = nil
}

func (rf *Raft) leaderHeartbeat() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.ServerRole == ServerLeader {
			// heartbeats
			// DPrintf("Server[%d] parallel send heartbeat", rf.me)
			rf.broadcastHeartbeat()
			// AppendEntries when last log index ≥ nextIndex for a follower
		}
		rf.mu.Unlock()
		ms := 150
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) checkApply() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.LastApplied >= rf.CommitIndex {
			rf.cond.Wait()
		}
		rf.LastApplied++
		DPrintf("Server[%d] apply idx[%d] logEntries=%s", rf.me, rf.LastApplied, Marshal(rf.LogEntries))
		msg := ApplyMsg{
			CommandValid: true,
			CommandIndex: rf.LastApplied,
			Command:      rf.LogEntries[rf.LastApplied].Cmd,
		}
		rf.mu.Unlock()
		rf.ApplyCh <- msg

		ms := 10
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) startCheckAppend() {
	for i := range rf.peers {
		if i != rf.me {
			go rf.checkAppend(i)
		}
	}
}

func (rf *Raft) checkAppend(server int) {
	for !rf.killed() {
		rf.mu.Lock()
		nextIdx := rf.NextIndex[server]
		lastLogIdx := rf.getLastLogIdx()
		prevLogIndex, prevLogTerm := rf.getPrevLogIdxAndTerm(nextIdx)
		entries := rf.LogEntries[nextIdx:]
		term := rf.CurrentTerm
		leaderCommit := rf.CommitIndex
		rf.mu.Unlock()
		if lastLogIdx >= nextIdx {
			// DPrintf("Server[%d] sendAppendEntries Entries=%s", rf.me, Marshal(args.Entries))
			success := rf.callAppendEntries(server, term, prevLogIndex, prevLogTerm, entries, leaderCommit)
			rf.mu.Lock()
			if term != rf.CurrentTerm {
				rf.mu.Unlock()
				continue
			}
			if success {
				rf.NextIndex[server] = nextIdx + len(entries)
				rf.MatchIndex[server] = prevLogIndex + len(entries)
				rf.mu.Unlock()
			} else {
				rf.NextIndex[server] = Max(rf.NextIndex[server]-1, 1)
				rf.mu.Unlock()
				continue
			}
		}
		ms := 10
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) checkCommit() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.CommitIndex < len(rf.LogEntries)-1 {
			n := rf.CommitIndex + 1
			count := 1
			for i := range rf.MatchIndex {
				if i == rf.me {
					continue
				}
				if rf.MatchIndex[i] >= n {
					count++
					if count > len(rf.peers)/2 && rf.LogEntries[n].Term == rf.CurrentTerm {
						DPrintf("Server[%d]  update commitIndx = %d", rf.me, n)
						rf.CommitIndex = n
						rf.cond.Broadcast()
						break
					}
				}
			}
		}
		rf.mu.Unlock()

		ms := 10
		time.Sleep(time.Duration(ms) * time.Millisecond)
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
	rf.mu = sync.Mutex{}

	// Your initialization code here (2A, 2B, 2C).
	rf.LogEntries = make([]*LogEntry, 0)
	rf.LogEntries = append(rf.LogEntries, &LogEntry{Index: 0, Term: 0})
	rf.NextIndex = make([]int, len(peers))
	rf.MatchIndex = make([]int, len(peers))
	rf.ServerRole = ServerFollower
	rf.LastHeartBeatTime = time.Now()
	rf.ElectionTimeout = time.Duration((500 + (rand.Int63() % 200))) * time.Millisecond
	rf.ApplyCh = applyCh
	rf.cond = sync.NewCond(&rf.mu)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.checkElection()
	go rf.checkApply()

	return rf
}
