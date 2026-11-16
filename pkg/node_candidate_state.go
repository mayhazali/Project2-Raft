package pkg

// doCandidate implements the logic for a Raft node in the candidate state.
func (n *Node) doCandidate() stateFunction {
	n.Out("Transitioning to CANDIDATE_STATE")
	n.setState(CandidateState)

	// TODO: Students should implement this method
	// Hint: perform any initial work, and then consider what a node in the
	// candidate state should do when it receives an incoming message on every
	// possible channel.
	for {
		select {
		case shutdown := <-n.gracefulExit:
			if shutdown {
				return nil
			}
		case msg := <-n.appendEntries:
			_, fallback := n.handleAppendEntries(msg)
			if fallback {
				return n.doFollower
			}
		case msg := <-n.requestVote:
			fallback := n.handleRequestVote(msg)
			if fallback {
				return n.doFollower
			}
		case msg := <-n.clientRequest:
			n.handleBadClientRequest(msg)
		case <-timeout:
            return n.doCandidate
		}
	}
}

// requestVotes is called to request votes from all other nodes. It takes in a
// channel on which the result of the vote should be sent over: true for a
// successful election, false otherwise.
func (n *Node) requestVotes(currTerm uint64) (fallback, electionResult bool) {
	// TODO: Students should implement this method
	return
}

// handleRequestVote handles an incoming vote request. It returns true if the caller
// should fall back to the follower state, false otherwise.
func (n *Node) handleRequestVote(msg RequestVoteMsg) (fallback bool) {
	// TODO: Students should implement this method
	fallback = false
	selfElectionTerm := n.GetCurrentTerm()
	candidateElectionTerm := msg.request.Term
	
	if selfElectionTerm < candidateElectionTerm {
		fallback = true
	}

	n.handleVote(msg)
	return fallback
}
