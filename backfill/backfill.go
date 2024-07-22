package backfill

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/api"
	"github.com/gh-efforts/rbot/repo"
	logging "github.com/ipfs/go-log/v2"
	"github.com/klauspost/compress/zstd"
)

var log = logging.Logger("backfill")

type Backfill struct {
	repo      *repo.Repo
	providers map[string]struct{}
}

type Filter struct {
	Providers  []string `json:"providers"`
	StartEpoch int      `json:"startEpoch"`
}

func New(repo *repo.Repo) *Backfill {
	b := &Backfill{
		repo:      repo,
		providers: map[string]struct{}{},
	}

	for p := range repo.Conf.Providers {
		b.providers[p] = struct{}{}
	}

	return b
}

func (b *Backfill) Fill(w http.ResponseWriter, r *http.Request) {
	var f Filter
	err := json.NewDecoder(r.Body).Decode(&f)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	err = b.parse(r.Context(), &f)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (b *Backfill) parse(ctx context.Context, f *Filter) error {
	providers := map[string]struct{}{}
	for _, p := range f.Providers {
		providers[p] = struct{}{}
	}
	if len(providers) == 0 {
		providers = b.providers
	}
	log.Debugw("filter", "providers", providers, "start-epoch", f.StartEpoch, "path", b.repo.StorageMarketDealFile())
	file, err := os.Open(b.repo.StorageMarketDealFile())
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}
	defer file.Close()

	dec, err := zstd.NewReader(file)
	if err != nil {
		return fmt.Errorf("zstd NewReader: %w", err)
	}
	defer dec.Close()

	decoder := json.NewDecoder(bufio.NewReader(dec))

	head, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("decoder head: %w", err)
	}

	if delim, ok := head.(json.Delim); !ok || delim != '{' {
		return fmt.Errorf("head expected {, got %v", delim)
	}

	for decoder.More() {
		select {
		case <-ctx.Done():
			return fmt.Errorf("context done")
		default:
		}
		var dealID int64
		t, err := decoder.Token()
		if err != nil {
			return fmt.Errorf("reading JSON key: %w", err)
		}

		var deal api.MarketDeal
		if err := decoder.Decode(&deal); err != nil {
			return fmt.Errorf("decode deal: %w", err)
		}

		key := t.(string)
		dealID, err = strconv.ParseInt(key, 10, 64)
		if err != nil {
			log.Debugf("dealID NAN: %s", key)
			continue
		}

		if _, ok := providers[deal.Proposal.Provider.String()]; !ok {
			//log.Debugf("%s not our providers", deal.Proposal.Provider.String())
			continue
		}
		if deal.Proposal.StartEpoch < abi.ChainEpoch(f.StartEpoch) {
			continue
		}
		label, err := deal.Proposal.Label.ToString()
		if err != nil {
			log.Debugf("deal: %d lable can not to string", label)
			continue
		}

		_, err = b.repo.DB.ExecContext(ctx, `INSERT or IGNORE INTO Deals (deal_id, payload_cid, client, provider, start_epoch, end_epoch) VALUES ($1, $2, $3, $4, $5, $6)`,
			dealID, label, deal.Proposal.Client.String(), deal.Proposal.Provider.String(), deal.Proposal.StartEpoch, deal.Proposal.EndEpoch)
		if err != nil {
			return err
		}
		log.Infow("backfill deal", "dealID", dealID, "client", deal.Proposal.Client.String(), "provider", deal.Proposal.Provider.String(), "label", label)
	}

	return nil
}
