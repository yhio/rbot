package retrieve

import (
	"context"
	"os"
	"sync"
	"time"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/lassie/pkg/indexerlookup"
	"github.com/filecoin-project/lassie/pkg/lassie"
	"github.com/filecoin-project/lassie/pkg/storage"
	ltypes "github.com/filecoin-project/lassie/pkg/types"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/gh-efforts/rbot/repo"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	trustlessutils "github.com/ipld/go-trustless-utils"
	"github.com/ipni/go-libipni/metadata"
)

var log = logging.Logger("retrieve")

type lotusApi interface {
	StateMinerInfo(context.Context, address.Address, types.TipSetKey) (api.MinerInfo, error)
}

type Retrieve struct {
	repo     *repo.Repo
	lotusApi lotusApi
	lassie   *lassie.Lassie
	indexer  *indexerlookup.IndexerCandidateSource
}

type task struct {
	dealID     int64
	payloadCID cid.Cid
	provider   address.Address
}

func New(ctx context.Context, repo *repo.Repo, lotusApi lotusApi) (*Retrieve, error) {
	lassie, err := lassie.NewLassie(ctx)
	if err != nil {
		return nil, err
	}

	indexer, err := indexerlookup.NewCandidateSource()
	if err != nil {
		return nil, err
	}

	r := &Retrieve{
		repo:     repo,
		lotusApi: lotusApi,
		lassie:   lassie,
		indexer:  indexer,
	}

	return r, nil
}

func (r *Retrieve) Run(ctx context.Context) {
	t := time.NewTicker(time.Duration(r.repo.Conf.Interval))
	defer t.Stop()

	for {
		select {
		case <-t.C:
			err := r.check(ctx)
			if err != nil {
				log.Error(err)
			}
		case <-ctx.Done():
			return
		}
	}
}

func (r *Retrieve) check(ctx context.Context) error {
	tasks, err := r.tasks(ctx)
	if err != nil {
		return err
	}

	var wg sync.WaitGroup
	throttle := make(chan struct{}, r.repo.Conf.Parallel)
	for _, t := range tasks {
		wg.Add(1)
		throttle <- struct{}{}

		go func(t *task) {
			defer wg.Done()
			defer func() {
				<-throttle
			}()

			err := r.retrieve(ctx, t)
			if err != nil {
				log.Error(err)
			}
		}(t)
	}
	wg.Wait()
	return nil
}

func (r *Retrieve) retrieve(ctx context.Context, t *task) error {
	log.Debugw("retrieving", "dealID", t.dealID)

	mi, err := r.lotusApi.StateMinerInfo(ctx, t.provider, types.EmptyTSK)
	if err != nil {
		return err
	}

	target := ltypes.RetrievalCandidate{}
	err = r.indexer.FindCandidates(ctx, t.payloadCID, func(rc ltypes.RetrievalCandidate) {
		if rc.MinerPeer.ID == *mi.PeerId {
			target = rc
		}
	})
	if err != nil {
		return err
	}

	log.Debugw("FindCandidates", "dealID", t.dealID, "target", target)

	if !target.RootCid.Equals(t.payloadCID) || target.MinerPeer.ID != *mi.PeerId {
		_, err := r.repo.DB.ExecContext(ctx, `UPDATE Deals SET indexer_result=$1, last_update=datetime('now') WHERE deal_id=$2`, "NOT_FOUND", t.dealID)
		if err != nil {
			return err
		}
		log.Infow("update deal", "dealID", t.dealID, "index_result", "NOT_FOUND")
		return nil
	}

	err = target.Metadata.Validate()
	if err != nil {
		return err
	}

	store := storage.NewDeferredStorageCar(os.TempDir(), target.RootCid)
	req, err := ltypes.NewRequestForPath(store, target.RootCid, "", trustlessutils.DagScopeBlock, nil)
	if err != nil {
		return err
	}

	protocols := []metadata.Protocol{}
	for _, mc := range target.Metadata.Protocols() {
		protocols = append(protocols, target.Metadata.Get(mc))

	}
	req.Providers = []ltypes.Provider{
		{
			Peer:      target.MinerPeer,
			Protocols: protocols,
		},
	}

	fetch_result := "OK"
	err_msg := ""
	stats, err := r.lassie.Fetch(ctx, req)
	if err != nil {
		log.Error(err)
		fetch_result = "ERR"
		err_msg = err.Error()
	}

	log.Debugw("fetch", "dealID", t.dealID, "fetch_result", fetch_result, "stats", stats)

	_, err = r.repo.DB.ExecContext(ctx, `UPDATE Deals SET indexer_result=$1, fetch_result=$2, err_msg=$3, last_update=datetime('now') WHERE deal_id=$4`, "OK", fetch_result, err_msg, t.dealID)
	if err != nil {
		return err
	}

	log.Infow("update deal", "dealID", t.dealID, "fetch_result", fetch_result, "err_msg", err_msg)
	return nil
}

func (r *Retrieve) tasks(ctx context.Context) ([]*task, error) {
	rows, err := r.repo.DB.QueryContext(ctx, `SELECT deal_id,payload_cid,provider FROM Deals WHERE last_update IS NULL OR DATE(last_update) < DATE('now')`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	tasks := []*task{}
	for rows.Next() {
		var dealID int64
		var payloadCID, provider string

		err := rows.Scan(&dealID, &payloadCID, &provider)
		if err != nil {
			return nil, err
		}

		payload, err := cid.Parse(payloadCID)
		if err != nil {
			log.Error(err)
			continue
		}
		addr, err := address.NewFromString(provider)
		if err != nil {
			log.Error(err)
			continue
		}
		tasks = append(tasks, &task{
			dealID:     dealID,
			payloadCID: payload,
			provider:   addr,
		})
	}
	log.Debugw("tasks", "counts", len(tasks))
	return tasks, nil
}
