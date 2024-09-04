package car

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"sync"

	"github.com/gh-efforts/rbot/post"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	carv1 "github.com/ipld/go-car"
	"github.com/ipld/go-car/v2/blockstore"
	"github.com/service-sdk/go-sdk-qn/v2/operation"
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

			payloadCid, block, err := fetchV3(ctx, info)
			if err != nil {
				log.Errorf("fetchv error: %v", err)
				return
			}

			err = c.post.PostRootBlock(payloadCid, block)
			if err != nil {
				log.Errorf("post block error: %v", err)
				return
			}

		}(carInfo)
	}

	return nil
}

func fetchV1(ctx context.Context, info CarInfo) (string, []byte, error) {
	payloadCid, err := cid.Parse(info.DataCid)
	if err != nil {
		return "", nil, err
	}

	reader, err := NewQNReadAt(info.FileName)
	if err != nil {
		return "", nil, err
	}

	br, err := blockstore.NewReadOnly(reader, nil)
	if err != nil {
		return "", nil, err
	}

	block, err := br.Get(ctx, payloadCid)
	if err != nil {
		return "", nil, err
	}

	roots, _ := br.Roots()
	log.Debugw("fetchV1", "payloadCid", payloadCid, "roots", roots)

	return payloadCid.String(), block.RawData(), nil
}

func fetchV2(ctx context.Context, info CarInfo) (string, []byte, error) {
	payloadCid, err := cid.Parse(info.DataCid)
	if err != nil {
		return "", nil, err
	}

	downloader := operation.NewDownloaderV2()
	resp, err := downloader.DownloadRaw(info.FileName, nil)
	if err != nil {
		return "", nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", nil, err
	}

	carReader := bytes.NewReader(body)
	br, err := blockstore.NewReadOnly(carReader, nil)
	if err != nil {
		return "", nil, err
	}

	block, err := br.Get(ctx, payloadCid)
	if err != nil {
		return "", nil, err
	}

	roots, _ := br.Roots()
	log.Debugw("fetchV2", "payloadCid", payloadCid, "roots", roots)

	return block.Cid().String(), block.RawData(), nil

}

func fetchV3(ctx context.Context, info CarInfo) (string, []byte, error) {
	payloadCid, err := cid.Parse(info.DataCid)
	if err != nil {
		return "", nil, err
	}

	downloader := operation.NewDownloaderV2()
	resp, err := downloader.DownloadRaw(info.FileName, nil)
	if err != nil {
		return "", nil, err
	}
	defer resp.Body.Close()

	carReader, err := carv1.NewCarReader(resp.Body)
	if err != nil {
		return "", nil, err
	}

	count := 0
	for {
		count++
		block, err := carReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", nil, err
		}
		if block.Cid() == payloadCid {
			log.Debugw("fetchV3", "payloadCid", payloadCid, "count", count, "roots", carReader.Header.Roots)
			return block.Cid().String(), block.RawData(), nil
		}
	}

	return "", nil, fmt.Errorf("block: %s not found", payloadCid)
}
