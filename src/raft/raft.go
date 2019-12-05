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

import "sync"
import "labrpc"
import "time"
import "math/rand"
import "fmt"

// import "bytes"
// import "encoding/gob"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type logEntry struct {
	Index       int             // first index is 1
	Term        int             // when entry was received by leader
	Command     interface{}     // for state machine
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	currentTerm     int         // Persistent state on all servers
	votedFor        int         // Persistent state on all servers, -1:voteNull
	log             []logEntry  // Persistent state on all servers

	commitIndex     int         // Volatile state on all servers
	lastApplied     int         // Volatile state on all servers

	nextIndex       []int       // Volatile state on leaders
	matchIndex      []int       // Volatile state on leaders

	leaderID            int
	state               int     // 0:follower 1:candidate 2:leader
	electionTimeout     time.Duration
	heartbeatTimeout    time.Duration
	electionTimer       *time.Timer
	heartbeatTimer      *time.Timer

	applyCh             chan ApplyMsg

	iskilled            bool    // for debug
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.

	term = rf.currentTerm
	isleader = (rf.leaderID == rf.me)

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
}




//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term            int     // candidate’s term
	CandidateID     int     // candidate requesting vote
	LastLogIndex    int     // index of candidate’s last log entry
	LastLogTerm     int     // term of candidate’s last log entry
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term            int     // currentTerm, for candidate to update itself
	VoteGranted     bool    // true means candidate received vote
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.iskilled {
		return
	}

	if args.Term > rf.currentTerm {                                     // Become follower
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
		rf.state = 0
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		rf.electionTimeout = getElectionTimeout()
		rf.electionTimer.Reset(rf.electionTimeout)
	}else if args.Term < rf.currentTerm {                               // Receiver implementation 1
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
	}else if rf.votedFor == -1 || rf.votedFor == args.CandidateID {     // Receiver implementation 2
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
		reply.Term = rf.currentTerm
		rf.electionTimeout = getElectionTimeout()
		rf.electionTimer.Reset(rf.electionTimeout)
	}

	fmt.Println("========----------",len(rf.log))
	if len(rf.log) > 0 {
		fmt.Println( "TERM:",rf.log[len(rf.log) - 1].Term,args.LastLogTerm)
		if rf.log[len(rf.log) - 1].Term > args.LastLogTerm {
			reply.VoteGranted = false
			rf.votedFor = -1
		} else if rf.log[len(rf.log) - 1].Term == args.LastLogTerm && len(rf.log) > args.LastLogIndex {
			reply.VoteGranted = false
			rf.votedFor = -1
		}
	}
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
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// AppendEntries RPC 
// AppendEntries RPC arguments structure.
type AppendEntriesArgs struct {
	Term        int         // leader’s term
	LeaderID    int
	PrevLogIndex    int
	PrevLogTerm     int
	Entries         []logEntry
	LeaderCommit    int	 						
}

// AppendEntries RPC reply structure.
type AppendEntriesReply struct {
	Term        int         // currentTerm, for leader to update itself
	Success     bool        // true if follower contained entry matching prevLogIndex and prevLogTerm
}

// AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.iskilled {
		return
	}

	// fmt.Println("RPC---Entries", args.Entries)


	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return		
	} else if args.Term >= rf.currentTerm {     // Become follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.leaderID = args.LeaderID
		rf.state = 0		
		reply.Term = rf.currentTerm
		rf.electionTimeout = getElectionTimeout()
		rf.electionTimer.Reset(rf.electionTimeout)

		// if rf.log != nil {
		// 	if args.LeaderCommit > rf.commitIndex {
		// 		if args.LeaderCommit < rf.log[len(rf.log) - 1].Index {
		// 			rf.commitIndex = args.LeaderCommit
		// 		} else {
		// 			rf.commitIndex = rf.log[len(rf.log) - 1].Index
		// 		}
		// 	}
		// }

		// if args.Entries != nil {
		
		if args.PrevLogIndex == 0 {
			reply.Success = true
		} else if len(rf.log) < args.PrevLogIndex {
			reply.Success = false
		} else if rf.log[args.PrevLogIndex - 1].Term != args.PrevLogTerm {
			reply.Success = false
		} else {
			reply.Success = true
		}

		if reply.Success {
			if args.Entries != nil {
				fmt.Println("RPC---rf.log:",rf.log,"\t",rf.commitIndex,"   ID:",rf.me)
				rf.log = rf.log[:args.PrevLogIndex]
				rf.log = append(rf.log, args.Entries...)
				fmt.Println("RPC---rf.log---after:",rf.log,"\t",rf.commitIndex,"   ID:",rf.me)
			}

			fmt.Println("============AAAAA-------------------", rf.commitIndex,args.LeaderCommit,args.PrevLogIndex,rf.me)
			if rf.log != nil {
				commitTmpIndex := -1
				if args.LeaderCommit > rf.commitIndex {
					if args.LeaderCommit < rf.log[len(rf.log) - 1].Index {
						fmt.Println("============BBBBBBB-------------------")
						// rf.commitIndex = args.LeaderCommit
						commitTmpIndex = args.LeaderCommit
					} else {
						fmt.Println("============CCCCCCCCC-------------------")
						// rf.commitIndex = rf.log[len(rf.log) - 1].Index
						commitTmpIndex = rf.log[len(rf.log) - 1].Index
					}
					// if rf.log[commitTmpIndex - 1].Term == args.PrevLogTerm {
					// 	rf.commitIndex = commitTmpIndex
					// } 
					if commitTmpIndex <= args.PrevLogIndex {
						fmt.Println("============hahhhhhh-------------------")
						rf.commitIndex = commitTmpIndex
					}
				}
			}

		}
		fmt.Println("heartbeat===rf.log:",rf.log,"\t",rf.commitIndex,"   ID:",rf.me,"  LeaderID",rf.leaderID,args.Term)

		// }

		// if rf.log != nil {
		// 	// var commitTmpIndex int
		// 	if args.LeaderCommit > rf.commitIndex {
		// 		if args.LeaderCommit < rf.log[len(rf.log) - 1].Index {
		// 			rf.commitIndex = args.LeaderCommit
		// 			// commitTmpIndex = args.LeaderCommit
		// 		} else {
		// 			rf.commitIndex = rf.log[len(rf.log) - 1].Index
		// 			// commitTmpIndex = rf.log[len(rf.log) - 1].Index
		// 		}
		// 		// if rf.log[commitTmpIndex - 1].Term == args.PrevLogTerm {
		// 		// 	rf.commitIndex = commitTmpIndex
		// 		// } 
		// 	}
		// }


		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			applyMessege := ApplyMsg{Index: rf.lastApplied, Command: rf.log[rf.lastApplied-1].Command}
			rf.applyCh <- applyMessege 
		}

		fmt.Println("RPC========APPLY",rf.lastApplied)
	}
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}




//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := -1
	isLeader := (rf.state == 2)

	fmt.Println("Start----SENDCCCCC",command)

	if isLeader {
		var logEntryNew logEntry
		logEntryNew.Index = len(rf.log) + 1
		logEntryNew.Term = rf.currentTerm
		logEntryNew.Command = command 
		rf.log = append(rf.log, logEntryNew)
		index = rf.log[len(rf.log) - 1].Index
		term = rf.log[len(rf.log) - 1].Term
		go rf.startAppendEntries()
	}

	fmt.Println("Start---log:",rf.log,"   meID:",rf.me,"   state:",rf.state,"   term",rf.currentTerm)


	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.iskilled = true
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

	// Your initialization code here.
	rf.currentTerm = 0
	rf.votedFor = -1    // vote for null
	rf.leaderID = -1    // don't know who is leader
	rf.state = 0        // init is follower
	rf.electionTimeout = getElectionTimeout()                       // 150ms~300ms
	rf.heartbeatTimeout = time.Duration(100) * time.Millisecond     // 100ms
	rf.electionTimer = time.NewTimer(rf.electionTimeout)
	rf.heartbeatTimer = time.NewTimer(rf.heartbeatTimeout)

    rf.nextIndex = make([]int, len(peers))
    rf.matchIndex = make([]int, len(peers))
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.leaderElection()

	return rf
}

func getElectionTimeout() time.Duration {
	// rand.Seed(time.Now().Unix())
	return time.Duration(150 + rand.Intn(150)) * time.Millisecond
}

func (rf *Raft) leaderElection() {
	for {
		if rf.iskilled {
			break
		}

		<-rf.electionTimer.C    // after rf.electionTimeout, time.newTimer send currentTime to rf.electionTimer.C
                                // in this duration, the process is trying to get rf.electionTimer.C
                                // if it succeed, it means no RPC to reset rf.electionTimer
		
		// become candidate
		rf.mu.Lock()
		rf.currentTerm++
		rf.state = 1
		rf.votedFor = rf.me
		rf.electionTimeout = getElectionTimeout()
		rf.electionTimer.Reset(rf.electionTimeout)
		rf.mu.Unlock()

		var voteCount int = 1
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}

			go func(rf *Raft, index int) {
				var newLogTerm int
				if len(rf.log) == 0 {
					newLogTerm = -1
				} else {
					newLogTerm = rf.log[len(rf.log) - 1].Term
				}
				args := RequestVoteArgs{Term: rf.currentTerm, CandidateID: rf.me,
				                        LastLogIndex: len(rf.log), LastLogTerm: newLogTerm}
				reply := RequestVoteReply{}
				comFlag := rf.sendRequestVote(index, args, &reply)  // if false: communication error
				if comFlag {
					rf.mu.Lock()
					defer rf.mu.Unlock()

					// Ensure that we're still a candidate
					if rf.state != 1 {
						return
					}

					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.state = 0
						rf.votedFor = -1
						rf.electionTimeout = getElectionTimeout()
						rf.electionTimer.Reset(rf.electionTimeout)
						return
					}

					if reply.VoteGranted {
						voteCount++
					}

					if voteCount > len(rf.peers)/2 {
						rf.state = 2
						rf.leaderID = rf.me
						rf.electionTimer.Stop()
						for i, _ := range rf.nextIndex {
							rf.nextIndex[i] = len(rf.log) + 1
						}
						go rf.startHeartbeat()
					}
				}
			}(rf, i)
		}
	}
}

func (rf *Raft) startHeartbeat() {
	rf.heartbeat()
	for {
		if rf.iskilled {
			break
		}
		if rf.state != 2 {
			break
		}
		<-rf.heartbeatTimer.C 
		rf.heartbeat()
	}
}

func (rf *Raft) heartbeat() {
	select {
		case <-rf.heartbeatTimer.C:
		default:
	}
	rf.heartbeatTimer.Reset(rf.heartbeatTimeout)
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(rf *Raft, peerID int) {
				for rf.nextIndex[peerID] > 0 {
					var newLogTerm int
					if rf.nextIndex[peerID] == 1 {
						newLogTerm = -1
					} else {
						newLogTerm = rf.log[rf.nextIndex[peerID] - 2].Term
					}
					args := AppendEntriesArgs{Term: rf.currentTerm, LeaderID: rf.me, 
					                          PrevLogIndex: rf.nextIndex[peerID] - 1,
					                          PrevLogTerm: newLogTerm, 
					                          LeaderCommit: rf.commitIndex}
					reply := AppendEntriesReply{}
					if rf.state != 2 {
						return
					}
					comFlag := rf.sendAppendEntries(peerID, args, &reply)  // if false: communication error
					if comFlag {
						// Ensure that we're still a leader
						if rf.state != 2 {
							return
						}
						
						if reply.Term > rf.currentTerm {
							rf.mu.Lock()
							defer rf.mu.Unlock()
							rf.currentTerm = reply.Term
							rf.state = 0
							rf.votedFor = -1
							rf.heartbeatTimer.Stop()
							rf.electionTimeout = getElectionTimeout()
							rf.electionTimer.Reset(rf.electionTimeout)
							return
						}

						if reply.Success == false {
							rf.mu.Lock()
							rf.nextIndex[peerID]--
							rf.mu.Unlock()
						} else {
							// rf.mu.Lock()
							// defer rf.mu.Unlock()
							// rf.matchIndex[peerID] = rf.nextIndex[peerID] - 1
							// rf.nextIndex[peerID] = rf.matchIndex[peerID] + 1
							return
						}
					} else {
						return
					}
				}
			}(rf, i)
		}
	}
}

func (rf *Raft) startAppendEntries() {
	var copyCount int = 1
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(rf *Raft, peerID int) {
				for rf.nextIndex[peerID] > 0 {
					var newLogTerm int
					if rf.nextIndex[peerID] == 1 {
						newLogTerm = -1
					} else {
						newLogTerm = rf.log[rf.nextIndex[peerID] - 2].Term
					}
					fmt.Println("===matchAndNext===:",rf.matchIndex[peerID],rf.nextIndex[peerID],peerID)
					args := AppendEntriesArgs{Term: rf.currentTerm, LeaderID: rf.me,
				    	                      PrevLogIndex: rf.nextIndex[peerID] - 1,
				        	                  PrevLogTerm: newLogTerm, 
				            	              Entries: rf.log[rf.nextIndex[peerID] - 1 :], 
				                	          LeaderCommit: rf.commitIndex}
				    fmt.Println("-------------Entries:",args.Entries, args.LeaderCommit)
					reply := AppendEntriesReply{}
					if rf.state != 2 {
						return
					}
					comFlag := rf.sendAppendEntries(peerID, args, &reply)  // if false: communication error
					if comFlag {
						// rf.mu.Lock()
						// defer rf.mu.Unlock()

						// Ensure that we're still a leader
						if rf.state != 2 {
							return
						}
						
						if reply.Term > rf.currentTerm {
							rf.mu.Lock()
							defer rf.mu.Unlock()
							rf.currentTerm = reply.Term
							rf.state = 0
							rf.votedFor = -1
							rf.heartbeatTimer.Stop()
							rf.electionTimeout = getElectionTimeout()
							rf.electionTimer.Reset(rf.electionTimeout)
							return
						}

						if reply.Success == false {
							rf.mu.Lock()
							rf.nextIndex[peerID]--
							rf.mu.Unlock()
						} else {
							rf.mu.Lock()
							defer rf.mu.Unlock()
							rf.matchIndex[peerID] = len(rf.log)
							rf.nextIndex[peerID] = rf.matchIndex[peerID] + 1
							fmt.Println("matchAndNext:",rf.matchIndex[peerID],rf.nextIndex[peerID],peerID)
							copyCount++
							if copyCount > len(rf.peers)/2 {
								rf.commitIndex = rf.log[len(rf.log) - 1].Index
								for rf.lastApplied < rf.commitIndex {
									rf.lastApplied++
									applyMessege := ApplyMsg{Index: rf.lastApplied, Command: rf.log[rf.lastApplied-1].Command}
									// fmt.Println("startRPC---applyMSG:",applyMessege)
									// select {
									// 	case <-rf.applyCh:
									// 	default:
									// }
									rf.applyCh <- applyMessege
								}
							}
							return
						}
					} else {
						return
					}
				}
			}(rf, i)
		}
	}
}