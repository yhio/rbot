package onchain

import (
	"bytes"
	"context"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/builtin"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/lotus/lib/must"
	"github.com/gh-efforts/rbot/repo"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/node/bindnode"
	"github.com/multiformats/go-multicodec"
)

var log = logging.Logger("onchain")

type lotusApi interface {
	SubscribeActorEventsRaw(ctx context.Context, filter *types.ActorEventFilter) (<-chan *types.ActorEvent, error)
	StateMarketStorageDeal(context.Context, abi.DealID, types.TipSetKey) (*api.MarketDeal, error)
}
type OnChain struct {
	repo      *repo.Repo
	lotusApi  lotusApi
	providers []uint64
}

func New(ctx context.Context, repo *repo.Repo, lotusApi lotusApi) (*OnChain, error) {
	log.Infow("OnChain", "providers", repo.Conf.Providers)

	providers := []uint64{}
	for _, c := range repo.Conf.Providers {
		a, err := address.NewFromString(c)
		if err != nil {
			return nil, err
		}
		id, err := address.IDFromAddress(a)
		if err != nil {
			return nil, err
		}
		providers = append(providers, id)
	}

	oc := &OnChain{
		repo:      repo,
		lotusApi:  lotusApi,
		providers: providers,
	}

	return oc, nil
}

func (oc *OnChain) SubscribeDealActivatedEvent(ctx context.Context) {
	providers := []types.ActorEventBlock{}
	for _, provider := range oc.providers {
		aeb := types.ActorEventBlock{
			Codec: uint64(multicodec.Cbor),
			Value: must.One(ipld.Encode(basicnode.NewInt(int64(provider)), dagcbor.Encode)),
		}
		providers = append(providers, aeb)
	}

	// subscribe event only to deal-activated events and specified providers from storage marker actor
	dealActivatedCbor := must.One(ipld.Encode(basicnode.NewString("deal-activated"), dagcbor.Encode))
	filter := &types.ActorEventFilter{
		Addresses: []address.Address{builtin.StorageMarketActorAddr},
		Fields: map[string][]types.ActorEventBlock{
			"$type": {
				{Codec: uint64(multicodec.Cbor), Value: dealActivatedCbor},
			},
		},
	}
	if len(providers) != 0 {
		filter.Fields["provider"] = providers
	}

	eventsChan, err := oc.lotusApi.SubscribeActorEventsRaw(ctx, filter)
	if err != nil {
		log.Error(err)
	}

	for event := range eventsChan {
		log.Debugw("event", "emiter", event.Emitter, "msg", event.MsgCid, "entries", len(event.Entries), "height", event.Height)

		var isDeal bool
		var id int64
		var client int64
		var provider int64
		for _, e := range event.Entries {
			//log.Debugw("entries", "key", e.Key)

			if e.Key == "$type" && bytes.Equal(e.Value, dealActivatedCbor) {
				isDeal = true
			} else if isDeal && e.Key == "id" {
				nd, err := ipld.DecodeUsingPrototype(e.Value, dagcbor.Decode, bindnode.Prototype((*int64)(nil), nil))
				if err != nil {
					log.Error(err)
					continue
				}
				id = *bindnode.Unwrap(nd).(*int64)
			} else if isDeal && e.Key == "client" {
				nd, err := ipld.DecodeUsingPrototype(e.Value, dagcbor.Decode, bindnode.Prototype((*int64)(nil), nil))
				if err != nil {
					log.Error(err)
					continue
				}
				client = *bindnode.Unwrap(nd).(*int64)
			} else if isDeal && e.Key == "provider" {
				nd, err := ipld.DecodeUsingPrototype(e.Value, dagcbor.Decode, bindnode.Prototype((*int64)(nil), nil))
				if err != nil {
					log.Error(err)
					continue
				}
				provider = *bindnode.Unwrap(nd).(*int64)
			} else {
				log.Warnw("not expected event", "Key", e.Key, "Codec", e.Codec)
			}
		}
		if isDeal && id != 0 && client != 0 && provider != 0 {
			ca, err := address.NewIDAddress(uint64(client))
			if err != nil {
				log.Error(err)
				continue
			}
			pa, err := address.NewIDAddress(uint64(provider))
			if err != nil {
				log.Error(err)
				continue
			}

			deal, err := oc.lotusApi.StateMarketStorageDeal(ctx, abi.DealID(id), types.EmptyTSK)
			if err != nil {
				log.Error(err)
				continue
			}

			if ca != deal.Proposal.Client || pa != deal.Proposal.Provider {
				log.Errorw("address not equal", "event client", ca, "event provider", pa, "deal client", deal.Proposal.Client, "deal provider", deal.Proposal.Provider)
				continue
			}

			label, err := deal.Proposal.Label.ToString()
			if err != nil {
				log.Error(err)
				continue
			}

			_, err = oc.repo.DB.ExecContext(ctx, `INSERT or IGNORE INTO Deals (deal_id, payload_cid, client, provider, start_epoch, end_epoch) VALUES ($1, $2, $3, $4, $5, $6)`,
				id, label, deal.Proposal.Client.String(), deal.Proposal.Provider.String(), deal.Proposal.StartEpoch, deal.Proposal.EndEpoch)
			if err != nil {
				log.Error(err)
				continue
			}

			log.Infow("insert deal", "dealID", id, "client", ca, "provider", pa, "label", label)
		}
	}
}
