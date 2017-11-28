package paxos

import "strings"
import "strconv"

// RPC args & replies
type PrepareArgs struct {
	Seq            int
	ProposalNumber int
}

type PrepareReply struct {
	OK                   bool
	Promise              int
	LastAcceptedProposal Proposal
}

type AcceptArgs struct {
	Seq      int
	Proposal Proposal
}

type AcceptReply struct {
	OK      bool
	MaxDone int // piggyback for cleaning
}

type DecidedArgs struct {
	Seq      int
	Proposal Proposal
}

type DecidedReply struct {
}

// definition of log entry

type entry struct {
	decided bool

	// Proposer
	proposalNumber int

	// Acceptor
	n_p              int      // highest number promised
	acceptedProposal Proposal // should update to the last one accepted
	decidedProposal  Proposal
	seq              int
}

func (ent *entry) decisionReached(proposal Proposal) {
	if !ent.decided {
		ent.decided = true
		ent.acceptedProposal = proposal
		ent.decidedProposal = proposal
	}
}

// definition of proposal

type Proposal struct {
	Number int         // proposal number (unqiue per Argeement instance)
	Value  interface{} // value of the Proposal
}

// proposal number as string format num_srv, so that ensures it is unique
type ProposalNumber string

func (a ProposalNumber) biggerOrEqual(b ProposalNumber) bool {
	ap := strings.Split(string(a), "_")
	bp := strings.Split(string(b), "_")

	an, anErr := strconv.Atoi(ap[0])
	as, asErr := strconv.Atoi(ap[1])
	bn, bnErr := strconv.Atoi(bp[0])
	bs, bsErr := strconv.Atoi(bp[1])

	if anErr != nil || asErr != nil || bnErr != nil || bsErr != nil {
		return false
	}

	return (an > bn) || (an == bn && as >= bs)
}

func (a ProposalNumber) bigger(b ProposalNumber) bool {
	ap := strings.Split(string(a), "_")
	bp := strings.Split(string(b), "_")

	an, anErr := strconv.Atoi(ap[0])
	as, asErr := strconv.Atoi(ap[1])
	bn, bnErr := strconv.Atoi(bp[0])
	bs, bsErr := strconv.Atoi(bp[1])

	if anErr != nil || asErr != nil || bnErr != nil || bsErr != nil {
		return false
	}

	return (an > bn) || (an == bn && as > bs)
}

func GenProposalNumber(num int, srv int) ProposalNumber {
	return ProposalNumber(strconv.Itoa(num) + "_" + strconv.Itoa(srv))
}
