package retrieve

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"sync"

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
	"github.com/robfig/cron/v3"
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

type ManualParam struct {
	Providers []string `json:"providers"`
	Limit     int      `json:"limit"`
	Parallel  int      `json:"parallel"`
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
	c := cron.New()
	_, err := c.AddFunc("CRON_TZ=Asia/Shanghai 30 01 * * *", func() {
		err := r.cronRetrieve(ctx)
		if err != nil {
			log.Error(err)
		}
		//TODO: gc: remove expired deal from db
	})
	if err != nil {
		panic(err)
	}
	c.Start()
}

func (r *Retrieve) cronRetrieve(ctx context.Context) error {
	log.Debug("cron retrieve start")

	providers, err := r.providers(ctx)
	if err != nil {
		return err
	}
	tasks, err := r.tasks(ctx, providers, r.repo.Conf.Limit)
	if err != nil {
		return err
	}

	err = r.retrieves(ctx, tasks, r.repo.Conf.Parallel)
	if err != nil {
		return err
	}

	return nil
}

func (r *Retrieve) ManualRetrieve(w http.ResponseWriter, req *http.Request) {
	var mp ManualParam
	err := json.NewDecoder(req.Body).Decode(&mp)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	err = r.manualRetrieve(req.Context(), &mp)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (r *Retrieve) manualRetrieve(ctx context.Context, mp *ManualParam) error {
	log.Debugw("manulRetrieve", "providers", mp.Providers, "limit", mp.Limit, "parallel", mp.Parallel)

	tasks, err := r.tasks(ctx, mp.Providers, mp.Limit)
	if err != nil {
		return err
	}

	err = r.retrieves(ctx, tasks, mp.Parallel)
	if err != nil {
		return err
	}
	return nil
}

func (r *Retrieve) retrieves(ctx context.Context, tasks []*task, parallel int) error {
	var wg sync.WaitGroup
	throttle := make(chan struct{}, parallel)
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
	defer store.Close()
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

func (r *Retrieve) tasks(ctx context.Context, providers []string, limit int) ([]*task, error) {
	tasks := []*task{}
	for _, p := range providers {
		rows, err := r.repo.DB.QueryContext(ctx, `SELECT deal_id,payload_cid,provider FROM Deals WHERE provider=$1 ORDER BY RANDOM() LIMIT $2`, p, limit)
		if err != nil {
			return nil, err
		}
		defer rows.Close()

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
	}

	log.Debugw("tasks", "providers", providers, "limit", limit, "counts", len(tasks))
	return tasks, nil
}

func (r *Retrieve) providers(ctx context.Context) ([]string, error) {
	rows, err := r.repo.DB.QueryContext(ctx, `SELECT DISTINCT provider FROM Deals`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	providers := []string{}
	for rows.Next() {
		var provider string
		err := rows.Scan(&provider)
		if err != nil {
			return nil, err
		}
		providers = append(providers, provider)
	}

	log.Debugf("provider: %s", providers)
	return providers, nil
}
