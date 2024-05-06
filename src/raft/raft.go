package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// create a new Raft server.
//		rf = Make(...)
// start agreement on a new log entry
//		rf.Start(command interface{}) (index, term, isleader)
// ask a Raft for its current term, and whether it thinks it is leader
//		rf.GetState() (term, isLeader)
// each time a new entry is committed to the log, each Raft peer should send
// an ApplyMsg to the service (or tester) in the same server.
//		ApplyMsg
//

import (
	"fmt"
	"labrpc"
	"math/rand"
	"strconv"
	"sync"
	"time"
)

// import "bytes"
// import "labgob"

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	applyCh chan ApplyMsg // Channel for the commit to the state machine

	// Your data here (3, 4).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	state   int // 0: follower, 1: candidate, 2: leader
	stopped bool

	// persistent state on all servers
	currentTerm int
	votedFor    int      //seems to be current leader id
	log         []string // not sure which type it should be

	// volatile state on all servers
	commitIndex int
	lastApplied int

	// volatile state on leaders
	nextIndex  []int
	matchIndex []int

	// heartbeat stuff
	heartbeatTimer int
	heartbeatChan  chan int

	// election timer stuff
	electionTimer int
}

// return currentTerm and whether this server
// believes it is the leader.

// MAYBE DONE
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool

	// Your code here (3).
	rf.mu.Lock()
	term = rf.currentTerm
	if rf.state == 2 {
		isleader = true
	} else {
		isleader = false
	}
	rf.mu.Unlock()

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (4).
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
	// Your code here (4).
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

// example RequestVote RPC arguments structure.
// field names must start with capital letters!

// PROBABLY DONE
type RequestVoteArgs struct {
	// Your data here (3, 4).

	// Figure 2 RequestVote RPC arguments
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!

// PROBABLY DONE
type RequestVoteReply struct {
	// Your data here (3).

	// Figure 2 RequestVote RPC replies
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	// arguments

	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []string
	LeaderCommit int
}

type AppendEntriesResults struct {
	// results
	Term    int
	Success bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3, 4).
	rf.mu.Lock()
	// no vote
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// vote
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && args.LastLogIndex >= rf.lastApplied {
		rf.votedFor = args.CandidateId
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		return
	}

	rf.mu.Unlock()

}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesResults) {
	// Your code here (3, 4).
	rf.mu.Lock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if args.PrevLogIndex > len(rf.log) {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	for index, entry := range args.Entries {
		if index > len(rf.log) {
			rf.log = append(rf.log, entry) // at the new index anyway
		} else {
			if rf.log[index] != entry {
				args.Entries = args.Entries[:index]
			}
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log))
	}

	rf.mu.Unlock()

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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, returns *AppendEntriesResults) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, returns)
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

	// Your code here (4).

	return index, term, isLeader
}

// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (rf *Raft) Kill() {
	// Your code here, if desired.
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

	// Your initialization code here (3, 4).
	rf.startInit()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

// check state conditions
func (rf *Raft) startInit() {
	rf.mu.Lock()

	for !rf.stopped {
		switch rf.state {
		case 0:
			// follower
			select {
			case <-time.After(time.Duration(rf.heartbeatTimer) * time.Millisecond):
				// how do you see if the heartbeat has timed out of the channel
				fmt.Printf("F%v hasn't received heartbeat. Stepping up to candidate C%v...", rf.me, rf.me)
				rf.state = 1

			// recieve a heartbeat
			case <-rf.heartbeatChan:
				fmt.Printf("F%v has received heartbeat. Staying as follower...", rf.me)
				rf.currentTerm += 1
			}
		case 1: // FINISH THIS
			// candidate
			select {
			case <-time.After(time.Duration(rf.heartbeatTimer) * time.Millisecond):

				// update term and vote for self
				rf.currentTerm += 1
				rf.votedFor = rf.me
				// add variable to keep track of num votes?

				// Reset election timer
				// TO DO : RESET ELECTION TIMER

				// Send vote requests
				num, err := strconv.Atoi(rf.log[len(rf.log)-1])
				if err != nil {
					num = 0 // we need to change this
				}

				args := RequestVoteArgs{
					Term:         rf.currentTerm,
					CandidateId:  rf.me,
					LastLogIndex: len(rf.log) - 1,
					LastLogTerm:  num,
				}

				reply := RequestVoteReply{
					Term:        rf.currentTerm,
					VoteGranted: false,
				}

				numVotes := 0

				for index, _ := range rf.peers {
					if index != rf.me {
						rf.sendRequestVote(index, &args, &reply)
					}

					if reply.VoteGranted {
						numVotes += 1
					}
				}

				// If majority of servers voted for me, become leader
				if numVotes > len(rf.peers)/2 {
					rf.state = 2
				}

				// If election timeout elapses: start new election
				// TO DO WITH TIMER

			// If AppendEntries RPC received from new leader: convert to follower
			case <-rf.heartbeatChan:
				rf.state = 0
			}

		case 2: // FINISH THIS
			// leader

			// Send heartbeats to all servers
			entryArgs := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: 0,
				PrevLogTerm:  0,
				Entries:      nil,
				LeaderCommit: 0,
			}

			entryReplies := make([]AppendEntriesResults, len(rf.peers))

			for index, _ := range rf.peers {
				if index != rf.me {
					rf.sendAppendEntries(index, &entryArgs, &entryReplies[index])
				}
			}

		}
	}

	rf.mu.Unlock()
}

func (rf *Raft) heartBeatTimeGenerator() {
	rf.heartbeatTimer = rand.Intn(100) + 100
}

func (rf *Raft) electionTimeGenerator() {
	rf.electionTimer = rand.Intn(100) + 100
}
