package car

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"os"
	"strconv"
	"sync"

	"github.com/gh-efforts/rbot/post"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-car/v2/blockstore"
)

var log = logging.Logger("car")

type Car struct {
	post *post.Post
}

type CarInfo struct {
	DataCid   string `json:"dataCid"`
	PieceCid  string `json:"pieceCid"`
	PieceSize int64  `json:"pieceSize"`
	CarSize   int64  `json:"carSize"`
	FileName  string `json:"fileName"`
}

func New(ctx context.Context, post *post.Post) (*Car, error) {
	return &Car{
		post: post,
	}, nil
}

func (c *Car) Fetch(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Query().Get("path")
	if path == "" {
		http.Error(w, "path is required", http.StatusBadRequest)
		return
	}

	parallel, err := strconv.Atoi(r.URL.Query().Get("parallel"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	log.Infow("fetch", "path", path, "parallel", parallel)
	err = c.fetch(r.Context(), path, parallel)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (c *Car) fetch(ctx context.Context, path string, parallel int) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	var wg sync.WaitGroup
	semaphore := make(chan struct{}, parallel)

	for scanner.Scan() {
		var carInfo CarInfo
		err := json.Unmarshal(scanner.Bytes(), &carInfo)
		if err != nil {
			log.Errorf("json unmarshal error: %v", err)
			continue
		}

		wg.Add(1)
		semaphore <- struct{}{}
		go func(info CarInfo) {
			defer wg.Done()
			defer func() { <-semaphore }()

			payloadCid, err := cid.Parse(info.DataCid)
			if err != nil {
				log.Errorf("parse data cid error: %v", err)
				return
			}

			reader, err := NewQNReadAt(info.FileName)
			if err != nil {
				log.Errorf("create qn reader error: %v", err)
				return
			}

			br, err := blockstore.NewReadOnly(reader, nil)
			if err != nil {
				log.Errorf("create blockstore error: %v", err)
				return
			}

			block, err := br.Get(ctx, payloadCid)
			if err != nil {
				log.Errorf("get block error: %v", err)
				return
			}

			err = c.post.PostRootBlock(payloadCid.String(), block.RawData())
			if err != nil {
				log.Errorf("post block error: %v", err)
				return
			}

			roots, err := br.Roots()
			log.Debugw("fetch", "payloadCid", payloadCid, "roots", roots, "pieceCid", info.PieceCid, "err", err)

		}(carInfo)
	}

	return nil
}
