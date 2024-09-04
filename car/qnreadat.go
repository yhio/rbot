package car

import (
	"fmt"
	"io"

	"github.com/service-sdk/go-sdk-qn/v2/operation"
)

type QNReadAt struct {
	key        string
	downloader *operation.Downloader
	size       int64
}

func NewQNReadAt(key string) (*QNReadAt, error) {
	downloader := operation.NewDownloaderV2()
	size, err := downloader.DownloadCheck(key)
	if err != nil {
		return nil, err
	}

	return &QNReadAt{
		key:        key,
		downloader: downloader,
		size:       size,
	}, nil
}

func (r *QNReadAt) ReadAt(p []byte, off int64) (n int, err error) {
	if off >= r.size {
		return 0, io.EOF
	}
	size := int64(len(p))
	if off+size > r.size {
		size = r.size - off
	}

	l, data, err := r.downloader.DownloadRangeBytes(r.key, off, size)
	if err != nil {
		return 0, err
	}
	if l != int64(len(data)) {
		return 0, fmt.Errorf("downloaded data size %d does not match %d", len(data), l)
	}

	return copy(p, data), nil
}

func (r *QNReadAt) Size() int64 {
	return r.size
}
