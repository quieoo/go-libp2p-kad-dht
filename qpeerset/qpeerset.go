package qpeerset

import (
	"math/big"
	"metrics"
	"sort"

	"github.com/libp2p/go-libp2p-core/peer"
	ks "github.com/whyrusleeping/go-keyspace"
)

// PeerState describes the state of a peer ID during the lifecycle of an individual lookup.
type PeerState int

const (
	// PeerHeard is applied to peers which have not been queried yet.
	PeerHeard PeerState = iota
	// PeerWaiting is applied to peers that are currently being queried.
	PeerWaiting
	// PeerQueried is applied to peers who have been queried and a response was retrieved successfully.
	PeerQueried
	// PeerUnreachable is applied to peers who have been queried and a response was not retrieved successfully.
	PeerUnreachable
)

// QueryPeerset maintains the state of a Kademlia asynchronous lookup.
// The lookup state is a set of peers, each labeled with a peer state.
type QueryPeerset struct {
	// the key being searched for
	key ks.Key

	// all known peers
	all []queryPeerState

	// sorted is true if all is currently in sorted order
	sorted bool
}

type queryPeerState struct {
	id         peer.ID
	distance   *big.Int
	state      PeerState
	referredBy peer.ID
}

type sortedQueryPeerset QueryPeerset

func (sqp *sortedQueryPeerset) Len() int {
	return len(sqp.all)
}

func (sqp *sortedQueryPeerset) Swap(i, j int) {
	sqp.all[i], sqp.all[j] = sqp.all[j], sqp.all[i]
}

func (sqp *sortedQueryPeerset) Less(i, j int) bool {
	if !metrics.CMD_PeerRH {
		di, dj := sqp.all[i].distance, sqp.all[j].distance
		return di.Cmp(dj) == -1
	} else {
		old_di := sqp.all[i].distance
		old_dj := sqp.all[j].distance
		di := metrics.GPeerRH.GetScore(sqp.all[i].distance, sqp.all[i].id.String())
		dj := metrics.GPeerRH.GetScore(sqp.all[j].distance, sqp.all[j].id.String())
		//fmt.Println("-----------------------------------------")
		//fmt.Printf("distance : \n%v\n%v\nid : \n%v\n%v\n", sqp.all[i].distance, sqp.all[j].distance,
		//	sqp.all[i].id, sqp.all[j].id)
		//fmt.Println(di)
		//fmt.Println(dj)
		//fmt.Println("-----------------------------------------")

		//fmt.Println("-----------------------------------------")
		//fmt.Println("bytes:")
		//fmt.Println(old_di.Bytes())
		//fmt.Println(old_dj.Bytes())
		//fmt.Println("公共前缀数，0的个数: ")
		//fmt.Println((32-len(old_di.Bytes()))*8 + ks.ZeroPrefixLen(old_di.Bytes()))
		//fmt.Println((32-len(old_dj.Bytes()))*8 + ks.ZeroPrefixLen(old_dj.Bytes()))
		//fmt.Println("-----------------------------------------")

		var isLess int
		if *di < *dj {
			isLess = -1
		} else if *di == *dj {
			isLess = 0
		} else {
			isLess = 1
		}

		if old_di.Cmp(old_dj)*isLess == -1 {
			metrics.GPeerRH.Compromise.Inc(1)
		}
		metrics.GPeerRH.AllCmp.Inc(1)

		return isLess == -1
	}
}

// NewQueryPeerset creates a new empty set of peers.
// key is the target key of the lookup that this peer set is for.
func NewQueryPeerset(key string) *QueryPeerset {
	return &QueryPeerset{
		key:    ks.XORKeySpace.Key([]byte(key)),
		all:    []queryPeerState{},
		sorted: false,
	}
}

func (qp *QueryPeerset) find(p peer.ID) int {
	for i := range qp.all {
		if qp.all[i].id == p {
			return i
		}
	}
	return -1
}

func (qp *QueryPeerset) distanceToKey(p peer.ID) *big.Int {
	// expDHT: 我们 在这里 计算 key 到 target 的距离
	return ks.XORKeySpace.Key([]byte(p)).Distance(qp.key)
}

// TryAdd adds the peer p to the peer set.
// If the peer is already present, no action is taken.
// Otherwise, the peer is added with state set to PeerHeard.
// TryAdd returns true iff the peer was not already present.
func (qp *QueryPeerset) TryAdd(p, referredBy peer.ID) bool {
	if qp.find(p) >= 0 {
		return false
	} else {
		qp.all = append(qp.all,
			queryPeerState{id: p, distance: qp.distanceToKey(p), state: PeerHeard, referredBy: referredBy})
		// fmt.Printf("querySet add (%s,%s,%d)\n", p, referredBy, qp.distanceToKey(p).Int64())
		qp.sorted = false
		return true
	}
}

func (qp *QueryPeerset) sort() {
	if qp.sorted {
		return
	}
	sort.Sort((*sortedQueryPeerset)(qp))
	qp.sorted = true
}

// SetState sets the state of peer p to s.
// If p is not in the peerset, SetState panics.
func (qp *QueryPeerset) SetState(p peer.ID, s PeerState) {
	// if s == PeerQueried {
	// 	fmt.Printf("set PeerQueried, %s\n", p)
	// }
	// if s == PeerUnreachable {
	// 	fmt.Printf("set unreachable, %s\n", p)
	// }
	qp.all[qp.find(p)].state = s
}

// GetState returns the state of peer p.
// If p is not in the peerset, GetState panics.
func (qp *QueryPeerset) GetState(p peer.ID) PeerState {
	return qp.all[qp.find(p)].state
}

// GetReferrer returns the peer that referred us to the peer p.
// If p is not in the peerset, GetReferrer panics.
func (qp *QueryPeerset) GetReferrer(p peer.ID) peer.ID {
	return qp.all[qp.find(p)].referredBy
}

// GetClosestNInStates returns the closest to the key peers, which are in one of the given states.
// It returns n peers or less, if fewer peers meet the condition.
// The returned peers are sorted in ascending order by their distance to the key.
func (qp *QueryPeerset) GetClosestNInStates(n int, states ...PeerState) (result []peer.ID) {
	// fmt.Printf("len of qp.all: %d\n", len(qp.all))
	qp.sort()
	m := make(map[PeerState]struct{}, len(states))
	for i := range states {
		m[states[i]] = struct{}{}
	}

	for _, p := range qp.all {
		if _, ok := m[p.state]; ok {
			result = append(result, p.id)
		}
	}
	if len(result) >= n {
		return result[:n]
	}
	return result
}

// GetClosestInStates returns the peers, which are in one of the given states.
// The returned peers are sorted in ascending order by their distance to the key.
func (qp *QueryPeerset) GetClosestInStates(states ...PeerState) (result []peer.ID) {
	return qp.GetClosestNInStates(len(qp.all), states...)
}

// NumHeard returns the number of peers in state PeerHeard.
func (qp *QueryPeerset) NumHeard() int {
	return len(qp.GetClosestInStates(PeerHeard))
}

// NumWaiting returns the number of peers in state PeerWaiting.
func (qp *QueryPeerset) NumWaiting() int {
	return len(qp.GetClosestInStates(PeerWaiting))
}
