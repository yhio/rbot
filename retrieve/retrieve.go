package retrieve

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/lassie/pkg/indexerlookup"
	"github.com/filecoin-project/lassie/pkg/lassie"
	"github.com/filecoin-project/lassie/pkg/storage"
	ltypes "github.com/filecoin-project/lassie/pkg/types"
	"github.com/gh-efforts/rbot/repo"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	trustlessutils "github.com/ipld/go-trustless-utils"
)

var log = logging.Logger("retrieve")

type Retrieve struct {
	repo    *repo.Repo
	lassie  *lassie.Lassie
	indexer *indexerlookup.IndexerCandidateSource
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

func New(ctx context.Context, repo *repo.Repo) (*Retrieve, error) {
	lassie, err := lassie.NewLassie(ctx)
	if err != nil {
		return nil, err
	}

	indexer, err := indexerlookup.NewCandidateSource()
	if err != nil {
		return nil, err
	}

	r := &Retrieve{
		repo:    repo,
		lassie:  lassie,
		indexer: indexer,
	}

	return r, nil
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

	var err error
	if mp.Providers == nil {
		mp.Providers, err = r.providers(ctx)
		if err != nil {
			return err
		}
	}

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
	store := storage.NewDeferredStorageCar(os.TempDir(), t.payloadCID)
	defer store.Close()

	req, err := ltypes.NewRequestForPath(store, t.payloadCID, "", trustlessutils.DagScopeBlock, nil)
	if err != nil {
		return err
	}

	req.Providers, err = ltypes.ParseProviderStrings(r.repo.Conf.Providers[t.provider.String()])
	if err != nil {
		return err
	}

	_, err = r.lassie.Fetch(ctx, req)
	if err != nil {
		if !strings.Contains(err.Error(), "there is no unsealed piece containing payload cid") {
			return err
		}
		//no unsealed
		_, err := r.repo.DB.ExecContext(ctx, `UPDATE Deals SET result=$1 WHERE deal_id=$2`, "NOUNSEALED", t.dealID)
		return err
	}

	block, err := store.Get(ctx, t.payloadCID.KeyString())
	if err != nil {
		return err
	}

	err = PostRootBlock(r.repo.Conf.ServerAddr, t.payloadCID.String(), block)
	if err != nil {
		return err
	}

	_, err = r.repo.DB.ExecContext(ctx, `UPDATE Deals SET result=$1 WHERE deal_id=$2`, "OK", t.dealID)
	if err != nil {
		return err
	}

	log.Debugw("update deal", "dealID", t.dealID, "result", "OK")
	return nil
}

func (r *Retrieve) tasks(ctx context.Context, providers []string, limit int) ([]*task, error) {
	tasks := []*task{}
	for _, p := range providers {
		var rows *sql.Rows
		var err error
		if limit == 0 {
			rows, err = r.repo.DB.QueryContext(ctx, `SELECT deal_id,payload_cid,provider FROM Deals WHERE provider=$1 AND result IS NULL`, p)
		} else {
			rows, err = r.repo.DB.QueryContext(ctx, `SELECT deal_id,payload_cid,provider FROM Deals WHERE provider=$1 AND result IS NULL LIMIT $2`, p, limit)
		}
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

type RootBlock struct {
	Root  string `json:"root"`
	Block []byte `json:"block"`
}

func PostRootBlock(addr string, root string, block []byte) error {
	rb := RootBlock{
		Root:  root,
		Block: block,
	}

	body, err := json.Marshal(&rb)
	if err != nil {
		return err
	}

	url := fmt.Sprintf("http://%s/block", addr)
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		r, err := io.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		return fmt.Errorf("status: %s msg: %s", resp.Status, string(r))
	}

	return nil
}
