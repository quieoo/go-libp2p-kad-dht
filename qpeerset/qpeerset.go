package qpeerset

import (
	"fmt"
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
	// expDHT: 也就是说，这是还没有 query 的
	PeerHeard PeerState = iota
	// PeerWaiting is applied to peers that are currently being queried.
	// expDHT: 也就是说，这是正在被 Query 的
	PeerWaiting
	// PeerQueried is applied to peers who have been queried and a response was retrieved successfully.
	// expDHT: 收到了回复
	PeerQueried
	// PeerUnreachable is applied to peers who have been queried and a response was not retrieved successfully.
	// expDHT: 没有收到回复
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
	// expDHT: 也就是说，我们这里再增加一个对时间的判断！
	//  或者如果不想改这个，就给之前设置一个评分
	if !metrics.CMD_PeerRH {
		di, dj := sqp.all[i].distance, sqp.all[j].distance
		return di.Cmp(dj) == -1
	} else {
		old_di := sqp.all[i].distance
		old_dj := sqp.all[j].distance

		di := metrics.GPeerRH.GetScore(sqp.all[i].distance, sqp.all[i].id.String())
		dj := metrics.GPeerRH.GetScore(sqp.all[j].distance, sqp.all[j].id.String())
		fmt.Println("-----------------------------------------")
		fmt.Printf("distance : \n%v\n%v\nid : \n%v\n%v\n", sqp.all[i].distance, sqp.all[j].distance,
			sqp.all[i].id, sqp.all[j].id)
		fmt.Println(di)
		fmt.Println(dj)
		fmt.Println("-----------------------------------------")

		if (old_di.Cmp(old_dj) * di.Cmp(dj)) == -1 {
			metrics.GPeerRH.Compromise.Inc(1)
		}
		metrics.GPeerRH.AllCmp.Inc(1)

		return di.Cmp(dj) == -1
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
		qp.sorted = false
		// expDHT: TODO：我们在这个时候要去更新一下参考的时间

		return true
	}
}

func (qp *QueryPeerset) sort() {
	if qp.sorted {
		return
	}
	// expDHT: 排序！？
	//  我们要定义 sortedQueryPeerset 中的 Len, Swap, Less 方法
	sort.Sort((*sortedQueryPeerset)(qp))
	qp.sorted = true
}

// SetState sets the state of peer p to s.
// If p is not in the peerset, SetState panics.
func (qp *QueryPeerset) SetState(p peer.ID, s PeerState) {
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
	// expDHT: 谁往 qp 里面添加的内容????????
	// expDHT: 首先做排序, 关键就在于怎么拍的序
	qp.sort()
	m := make(map[PeerState]struct{}, len(states))
	for i := range states {
		m[states[i]] = struct{}{}
	}

	//
	for _, p := range qp.all {
		if _, ok := m[p.state]; ok {
			// expDHT: 是我们想要的 state
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
