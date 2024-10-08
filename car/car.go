package car

import (
	"bufio"
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
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/service-sdk/go-sdk-qn/v2/operation"
)

var log = logging.Logger("car")

type Car struct {
	post *post.Post
	mc   *MinioConfig
}

type CarInfo struct {
	DataCid   string `json:"dataCid"`
	PieceCid  string `json:"pieceCid"`
	PieceSize int64  `json:"pieceSize"`
	CarSize   int64  `json:"carSize"`
	FileName  string `json:"fileName"`
}

func New(ctx context.Context, post *post.Post) (*Car, error) {
	mc, err := getMinioConfig()
	if err != nil {
		return nil, err
	}

	return &Car{
		post: post,
		mc:   mc,
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

			payloadCid, block, err := c.fetchRootBlock(ctx, info)
			if err != nil {
				log.Errorf("%s fetch error: %v", info.FileName, err)
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

func (c *Car) fetchRootBlock(ctx context.Context, info CarInfo) (string, []byte, error) {
	payloadCid, err := cid.Parse(info.DataCid)
	if err != nil {
		return "", nil, err
	}

	var carReader *carv1.CarReader
	if c.mc != nil {
		minioClient, err := minio.New(c.mc.Endpoint, &minio.Options{
			Creds:  credentials.NewStaticV4(c.mc.AccessKey, c.mc.SecretKey, ""),
			Secure: c.mc.UseSSL,
			Region: c.mc.Region,
		})
		if err != nil {
			return "", nil, err
		}
		object, err := minioClient.GetObject(ctx, c.mc.Bucket, info.FileName, minio.GetObjectOptions{})
		if err != nil {
			return "", nil, err
		}
		defer object.Close()

		carReader, err = carv1.NewCarReader(object)
		if err != nil {
			return "", nil, err
		}

	} else {
		downloader := operation.NewDownloaderV2()
		resp, err := downloader.DownloadRaw(info.FileName, nil)
		if err != nil {
			return "", nil, err
		}
		defer resp.Body.Close()

		carReader, err = carv1.NewCarReader(resp.Body)
		if err != nil {
			return "", nil, err
		}
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
			log.Debugw("fetchRootBlock", "payloadCid", payloadCid, "count", count, "roots", carReader.Header.Roots)
			return block.Cid().String(), block.RawData(), nil
		}
	}

	return "", nil, fmt.Errorf("block: %s not found", payloadCid)
}
