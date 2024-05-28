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
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"project3/src/labgob"
	"project3/src/labrpc"
	"sync"
	"sync/atomic"
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
	votedFor    int   //seems to be current leader id
	log         []Log // not sure which type it should be

	// volatile state on all servers
	commitIndex int
	lastApplied int

	// volatile state on leaders
	nextIndex  []int
	matchIndex []int

	// heartbeat stuff
	heartbeatTimer int
	heartbeatChan  chan AppendEntriesArgs
	stopCh         chan bool

	// election stuff
	electionTimer int
	chWinElection chan bool
}

var commandIdx int = 0 // global command index

type Log struct {
	Command interface{}
	Term    int
}

// return currentTerm and whether this server
// believes it is the leader.

// MAYBE DONE
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool

	// Your code here (3).
	rf.mu.Lock()
	//rf.readPersist(rf.persister.ReadRaftState())
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

	w := new(bytes.Buffer)    // holds the encrypted state
	e := labgob.NewEncoder(w) // encoder (we need from labgob)
	e.Encode(rf.currentTerm)  // encode the term, votedFor, and log
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()                // convert buffer to byte slice (wtf is that)
	rf.persister.SaveRaftState(data) // save the state to persister

}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (4).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currTerm int
	var votedFor int
	var log []Log
	if d.Decode(&currTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil {
		fmt.Println(errors.New("error decoding state"))
	} else {
		rf.currentTerm = currTerm
		rf.votedFor = votedFor
		rf.log = log
	}
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
	Entries      []Log
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
	defer rf.mu.Unlock()

	fmt.Printf("server %v received a vote request\n", rf.me)

	// PROJECT 4 CODE //////////////////////////////////////////////
	// no vote --> term of last long entry has to be up-to-date
	//rf.readPersist(rf.persister.ReadRaftState())

	if args.LastLogTerm < rf.log[len(rf.log)-1].Term {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if len(rf.log)-1 > args.LastLogIndex {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false // log is not up-to-date
		return
	}

	///////////////////////////////////////////////////////////////

	// update rf term if the candidate has higher term
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}
	//rf.readPersist(rf.persister.ReadRaftState())
	// vote
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		fmt.Printf("server %v granted a vote request\n", rf.me)
		rf.votedFor = args.CandidateId
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.persist()
		rf.electionTimeGenerator()
		return
	} else {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesResults) {
	// Your code here (3, 4).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	fmt.Printf("Follower %d receiving AppendEntries; Term: %d, LeaderId: %d, PrevLogIndex: %d, Entries: %v\n",
		rf.me, args.Term, args.LeaderId, args.PrevLogIndex, args.Entries)

	// we need to go back and double check the indexing here (if index out of bounds look here)

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// if prev long term doesn't match

	if args.PrevLogIndex >= 0 && args.PrevLogIndex < len(rf.log) {
		if args.PrevLogTerm != rf.log[args.PrevLogIndex].Term {
			reply.Term = rf.currentTerm
			reply.Success = false
			return
		}
	}

	// step down to follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = 0
		rf.persist()
	}

	// if existing entry conflicts with new one, delete existing and all that follow it
	for index, entry := range args.Entries {
		pos := args.PrevLogIndex + 1 + index
		if pos < len(rf.log) {
			if rf.log[pos].Term != entry.Term {
				rf.log = rf.log[:pos] // truncate the log
			}
		}
		rf.log = append(rf.log, entry) // append new entries not already in the log
		rf.persist()
		fmt.Printf("appended a new entry\n")
	}

	// leader commit > rf commit index
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
		rf.fixLogs()
	}

	reply.Success = true
	reply.Term = rf.currentTerm

	rf.heartbeatChan <- *args
	fmt.Printf("Follower %d appended entries and updated commit index to %d\n", rf.me, rf.commitIndex)
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
	//fmt.Printf("Follower %d received heartbeat from Leader %d; Term: %d\n", server, args.LeaderId, args.Term)
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
	isLeader := rf.safeGetState() == 2

	// Your code here (4).
	// if not leader, return false

	if !isLeader {
		return index, term, isLeader
	}

	// POTENTIALLY WRONG

	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.log = append(rf.log, Log{command, rf.currentTerm})
	index = len(rf.log) - 1
	term = rf.currentTerm
	rf.persist()
	rf.broadcastCommand()

	// add command to log
	// if rf.safeGetState() == 2 {
	// 	rf.mu.Lock()
	// 	rf.log = append(rf.log, Log{command, rf.currentTerm})
	// 	index = len(rf.log)
	// 	term = rf.currentTerm
	// 	rf.persist()
	// 	rf.mu.Unlock()

	// 	if rf.commitIndex > rf.lastApplied {
	// 		msg := ApplyMsg{
	// 			CommandValid: true,
	// 			Command:      command,
	// 			CommandIndex: rf.lastApplied + 1,
	// 		}
	// 		rf.mu.Lock()
	// 		rf.applyCh <- msg
	// 		commandIdx += 1
	// 		rf.lastApplied += 1
	// 		rf.persist()
	// 		rf.mu.Unlock()
	// 	}

	// 	// heartbeat with the client command to all followers
	// 	rf.broadcastCommand()
	// }
	return index, term, isLeader
}

// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (rf *Raft) Kill() {
	// Your code here, if desired.

	rf.mu.Lock()
	rf.stopped = true
	rf.mu.Unlock()
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
	rf.mu.Lock()
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.state = 0

	// Your initialization code here (3, 4).
	rf.heartbeatChan = make(chan AppendEntriesArgs)
	rf.chWinElection = make(chan bool, 1)
	rf.stopCh = make(chan bool)

	// project 4 initializations
	rf.applyCh = applyCh
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.log = make([]Log, 0)
	rf.log = append(rf.log, Log{nil, 0})
	rf.mu.Unlock()

	rf.electionTimeGenerator()
	rf.heartBeatTimeGenerator()

	go func() {
		rf.startInit()
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

// check state conditions
func (rf *Raft) startInit() {
	rf.mu.Lock()
	rf.stopped = false
	rf.mu.Unlock()

	for !rf.stopped {
		switch rf.state {
		case 0:
			// follower
			select {

			// leader failure
			case <-time.After(time.Duration(rf.safeGetElectionTime()) * time.Millisecond):
				// heartbeat timeout case
				fmt.Printf("F%v hasn't received heartbeat. Stepping up to candidate C%v...\n", rf.me, rf.me)
				rf.mu.Lock()
				rf.state = 1
				rf.mu.Unlock()

				// go to elections right away
				rf.elections()

			// recieve a heartbeat
			case heartbeat := <-rf.heartbeatChan:
				fmt.Printf("F%v has received heartbeat. Staying as follower...\n", rf.me)
				rf.electionTimeGenerator()

				// if rf.lastApplied < heartbeat.LeaderCommit {
				// 	nextAdd := rf.log[rf.lastApplied+1 : heartbeat.LeaderCommit+1]
				// 	for _, entry := range nextAdd {
				// 		msg := ApplyMsg{
				// 			CommandValid: true,
				// 			Command:      entry.Command,
				// 			CommandIndex: rf.lastApplied + 1,
				// 		}
				// 		rf.applyCh <- msg
				// 		commandIdx += 1
				// 		rf.lastApplied += 1
				// 	}
				// }

				if heartbeat.Term > rf.safeGetTerm() {
					rf.mu.Lock()
					rf.currentTerm += 1
					rf.persist()
					rf.mu.Unlock()
				}
			}
		case 1:
			// candidate
			fmt.Printf("C%v hasn't received heartbeat. Starting initial election C%v...\n", rf.me, rf.me)

			select {
			case <-time.After(time.Duration(rf.safeGetElectionTime()) * time.Millisecond):
				fmt.Printf("C%v election has timed out. Start new election.\n", rf.me)
				// start elections
				go rf.elections()

			case heartbeat := <-rf.heartbeatChan:
				if heartbeat.Term > rf.safeGetTerm() {
					fmt.Printf("C%v has received heartbeat. Stepping down to follower F%v...\n", rf.me, rf.me)
					rf.mu.Lock()
					rf.state = 0
					rf.mu.Unlock()
				}

			case win := <-rf.chWinElection:
				if win {
					rf.mu.Lock()
					// change state
					rf.state = 2

					// reinitialize nextIndex and matchIndex
					//rf.readPersist(rf.persister.ReadRaftState())
					for i := range rf.nextIndex {
						rf.nextIndex[i] = len(rf.log)
						rf.matchIndex[i] = 0
					}

					rf.mu.Unlock()

					fmt.Printf("Candidate %d won the election and is now the Leader.\n", rf.me)
					// go rf.sendHeartBeats()
					go rf.broadcastHeartbeat()
				}
			}

		case 2:
			// leader
			select {
			case <-time.After((time.Duration(100)) * time.Millisecond):
				rf.mu.Lock()
				if rf.state != 2 {
					fmt.Printf("L%v NOT THE LEADER", rf.me)
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()
				rf.broadcastHeartbeat()
			case heartbeat := <-rf.heartbeatChan:
				if heartbeat.Term > rf.safeGetTerm() {
					fmt.Printf("L%v has received heartbeat. Stepping down to follower F%v...\n", rf.me, rf.me)
					rf.mu.Lock()
					rf.state = 0
					rf.currentTerm = heartbeat.Term
					rf.persist()
					rf.mu.Unlock()
				}
			}
		}
	}
}

func (rf *Raft) safeGetTerm() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//rf.readPersist(rf.persister.ReadRaftState())
	return rf.currentTerm
}

func (rf *Raft) safeGetState() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.state
}

func (rf *Raft) heartBeatTimeGenerator() {
	rf.mu.Lock()
	rf.heartbeatTimer = rand.Intn(100) + 200
	rf.mu.Unlock()
}

func (rf *Raft) safeGetHeartBeatTime() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.heartbeatTimer
}

func (rf *Raft) electionTimeGenerator() {
	rf.electionTimer = rand.Intn(200) + 300
}

func (rf *Raft) safeGetElectionTime() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.electionTimer
}

func (rf *Raft) elections() {
	rf.mu.Lock()
	fmt.Printf("Node %d starting election for term %d\n", rf.me, rf.currentTerm+1)
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.persist()
	rf.mu.Unlock()

	rf.electionTimeGenerator()

	votes := int32(1) // start at 1 because voted for self
	voteChannels := make(chan bool, len(rf.peers)-1)

	for index := range rf.peers {
		if index != rf.me {

			go func(server int) {

				// making sure we don't overflow the log
				var lli int
				var llt int
				//rf.readPersist(rf.persister.ReadRaftState())
				if (len(rf.log) - 1) < 0 {
					lli = 0
					llt = 0
				} else {
					lli = len(rf.log) - 1
					llt = rf.log[len(rf.log)-1].Term
				}

				// get votes
				//rf.readPersist(rf.persister.ReadRaftState())
				requestArgs := RequestVoteArgs{
					Term:         rf.currentTerm,
					CandidateId:  rf.me,
					LastLogIndex: lli,
					LastLogTerm:  llt,
				}
				reply := RequestVoteReply{}
				if ok := rf.sendRequestVote(server, &requestArgs, &reply); ok && reply.VoteGranted {
					voteChannels <- true
				} else {
					voteChannels <- false
				}
			}(index)
		}
	}

	// PROTECTED, ALWAYS UPDATING VOTES WHILE STILL SENNDING OUT VOTE REQUESTS
	go func() {
		for vote := range voteChannels {
			if vote {
				atomic.AddInt32(&votes, 1)
			}
		}
	}()

	// if winner then setup new leader, else do nothing -- set a max timeout so it doesn't run forever
	go func() {
		maxTimeOut := time.After(time.Duration(500) * time.Millisecond)
		for {
			select {
			case <-maxTimeOut:
				fmt.Printf("Node %d timed out\n", rf.me)
				return
			default:
				if atomic.LoadInt32(&votes) > int32(len(rf.peers)/2) {
					fmt.Printf("Node %d received %d votes\n", rf.me, votes)
					fmt.Printf("Node %d becomes leader\n", rf.me)
					go func() { rf.chWinElection <- true }() //do this in loop above
					return
				}
			}
		}
	}()
}

func (rf *Raft) broadcastHeartbeat() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != 2 {
		rf.mu.Unlock()
		return
	}

	fmt.Printf("L%v is sending out heartbeats. Staying the leader L%v...\n", rf.me, rf.me)
	var pli int // need to make sure we don't overflow
	var plt int
	//rf.readPersist(rf.persister.ReadRaftState())
	if (len(rf.log) - 1) < 0 {
		pli = 0
		plt = 0
	} else {
		pli = len(rf.log) - 1
		plt = rf.log[len(rf.log)-1].Term
	}
	//rf.readPersist(rf.persister.ReadRaftState())
	// Send heartbeats to all servers
	entryArgs := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: pli,
		PrevLogTerm:  plt,
		Entries:      nil,
		LeaderCommit: rf.commitIndex,
	}

	for index := range rf.peers {
		if index != rf.me {
			rf.mu.Unlock()
			go func(server int) {
				reply := AppendEntriesResults{}
				ok := rf.sendAppendEntries(server, &entryArgs, &reply)

				fmt.Printf("Leader %d sent heartbeat to server %d: ok=%v, success=%v\n", rf.me, server, ok, reply.Success)

				if ok {
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.state = 0
						rf.votedFor = -1
						rf.persist()
					}
				}
			}(index)
		}
	}
	rf.mu.Unlock()
}

func (rf *Raft) broadcastCommand() {
	rf.mu.Lock()
	if rf.state != 2 {
		rf.mu.Unlock()
		return
	}
	defer rf.mu.Unlock()

	fmt.Printf("L%v is sending out heartbeats. Staying the leader L%v...\n", rf.me, rf.me)
	var pli int // need to make sure we don't overflow
	var plt int
	//rf.readPersist(rf.persister.ReadRaftState())
	if (len(rf.log) - 2) < 0 {
		pli = 0
		plt = 0
	} else {
		pli = len(rf.log) - 2
		plt = rf.log[len(rf.log)-2].Term
	}

	// success := int32(0) // start at 0 because sending heartbeats
	// heartbeatChan := make(chan bool, len(rf.peers)-1)

	// // do the n check from figure 2
	// n := len(rf.log) - 1
	// match_count := 0
	// for n >= 0 {
	// 	for idx := range rf.peers {
	// 		if rf.matchIndex[idx] >= n {
	// 			match_count += 1
	// 		}
	// 	}

	// 	if match_count >= len(rf.peers)/2 && rf.log[n].Term == rf.currentTerm && n > rf.commitIndex {
	// 		rf.commitIndex = n
	// 		break
	// 	}
	// 	n -= 1
	// }

	for index := range rf.peers {
		if index != rf.me {

			go func(server int) {
				// Send heartbeats to all servers

				//rf.readPersist(rf.persister.ReadRaftState())
				entryArgs := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: pli,
					PrevLogTerm:  plt,
					Entries:      rf.log[rf.nextIndex[server]:],
					LeaderCommit: rf.commitIndex,
				}
				reply := AppendEntriesResults{}

				// //rf.readPersist(rf.persister.ReadRaftState())
				// if len(rf.log)-1 >= rf.nextIndex[server] && rf.nextIndex[server] >= 0 {
				// 	nextIndex := rf.nextIndex[server]
				// 	entryArgs.Entries = rf.log[nextIndex:]
				// }
				rf.mu.Unlock()
				if ok := rf.sendAppendEntries(server, &entryArgs, &reply); ok && reply.Success {
					// heartbeatChan <- true
					rf.nextIndex[server] = rf.nextIndex[server] + len(entryArgs.Entries) // might be rf.log length
					rf.matchIndex[server] = rf.nextIndex[server] - 1
					rf.fixCommitIndex()
					//rf.readPersist(rf.persister.ReadRaftState())

					// SEND THE MESSAGE AFTER THE CHECK FOR OVER HALF

				} else {
					// heartbeatChan <- false
					rf.nextIndex[server] -= 1
					//rf.readPersist(rf.persister.ReadRaftState())
					// if len(rf.log)-1 >= rf.nextIndex[server] && rf.nextIndex[server] >= 0 {
					// 	nextIndex := rf.nextIndex[server]
					// 	entryArgs.Entries = rf.log[nextIndex:]
					// }
					// rf.sendAppendEntries(server, &entryArgs, &reply) // decrement next index and try again
				}
				fmt.Printf("Leader %d sent AppendEntries to server %d: success=%v\n", rf.me, server, reply.Success)
			}(index)
		}
	}

	// go func() {
	// 	for heartbeat_true := range heartbeatChan {
	// 		if heartbeat_true {
	// 			atomic.AddInt32(&success, 1)
	// 		}
	// 	}
	// }()

	// go func() {
	// 	maxTimeOut := time.After(time.Duration(500) * time.Millisecond)
	// 	for {
	// 		select {
	// 		case <-maxTimeOut:
	// 			fmt.Printf("Leader %d timed out on receiving responses to heartbeats\n", rf.me)
	// 			return
	// 		default:
	// 			if atomic.LoadInt32(&success) > int32(len(rf.peers)/2) {
	// 				fmt.Printf("Leader %d received %d replies from heartbeats\n", rf.me, success)
	// 				fmt.Printf("COMMITTEEEEDDDD!!!\n")
	// 				if rf.commitIndex > rf.lastApplied {
	// 					msg := ApplyMsg{
	// 						CommandValid: true,
	// 						Command:      rf.log[len(rf.log)-1].Command,
	// 						CommandIndex: commandIdx,
	// 					}
	// 					rf.applyCh <- msg
	// 					commandIdx += 1
	// 					rf.mu.Lock()
	// 					rf.lastApplied += 1
	// 					rf.commitIndex += 1 //maintained by leader
	// 					rf.persist()
	// 					rf.mu.Unlock()
	// 				}
	// 				return
	// 			}
	// 		}
	// 	}
	// }()
}

// this is going to send the messages
func (rf *Raft) fixLogs() {
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied += 1
		msg := ApplyMsg{
			CommandValid: true,
			Command:      rf.log[rf.lastApplied].Command,
			CommandIndex: rf.lastApplied,
		}
		rf.applyCh <- msg
	}
}

// this is going to fix the commit index
func (rf *Raft) fixCommitIndex() {
	for n := len(rf.log) - 1; n > rf.commitIndex; n-- {
		count := 1
		for index := range rf.peers {
			if index != rf.me && rf.matchIndex[index] >= n {
				count += 1
			}
		}

		if count > len(rf.peers)/2 && rf.log[n].Term == rf.currentTerm {
			rf.commitIndex = n
			break

		}
	}
	rf.fixLogs()
}

// lastApplied --> last index that has been added to the state machines, what entry has been applied to that machine
// leaders and followers can apply everything between this and the commit index

// applyCh updates the client
// check the lastApplied index and check the commit Index --> if commit > LA, then send those entries
// done by both leaders and followers
