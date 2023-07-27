package dht

import (
	"context"
	"fmt"
	"metrics"
	"time"

	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/routing"

	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	kb "github.com/libp2p/go-libp2p-kbucket"
)

// GetClosestPeers is a Kademlia 'node lookup' operation. Returns a channel of
// the K closest peers to the given key.
//
// If the context is canceled, this function will return the context error along
// with the closest K peers it has found so far.
func (dht *IpfsDHT) GetClosestPeers(ctx context.Context, key string) (<-chan peer.ID, error) {
	if key == "" {
		return nil, fmt.Errorf("can't lookup empty key")
	}
	//TODO: I can break the interface! return []peer.ID
	lookupRes, err := dht.runLookupWithFollowup(ctx, key,
		func(ctx context.Context, p peer.ID) ([]*peer.AddrInfo, error) {

			timectx, _ := context.WithTimeout(ctx, time.Duration(metrics.QueryPeerTime)*time.Second)
			// For DHT query command
			routing.PublishQueryEvent(ctx, &routing.QueryEvent{
				Type: routing.SendingQuery,
				ID:   p,
			})
			startT := time.Now()
			pmes, err := dht.findPeerSingle(timectx, p, peer.ID(key))
			if err != nil {
				//cpllogger.Debugf("findPeerSingle error %s", err.Error())
				logger.Debugf("error getting closer peers: %s", err)
				return nil, err
			}
			tDur := time.Since(startT)
			if metrics.CMD_PeerRH {
				metrics.GPeerRH.Update(p.String(), tDur)
			}
			peers := pb.PBPeersToPeerInfos(pmes.GetCloserPeers())

			// For DHT query command
			routing.PublishQueryEvent(ctx, &routing.QueryEvent{
				Type:      routing.PeerResponse,
				ID:        p,
				Responses: peers,
			})

			return peers, err
		},
		func() bool { return false },
		true,
	)

	if err != nil {
		return nil, err
	}

	out := make(chan peer.ID, dht.bucketSize)
	defer close(out)

	for _, p := range lookupRes.peers {
		out <- p
	}
	if ctx.Err() == nil && lookupRes.completed {
		// refresh the cpl for this key as the query was successful
		dht.routingTable.ResetCplRefreshedAtForID(kb.ConvertKey(key), time.Now())
	}
	return out, ctx.Err()
}
