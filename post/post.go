package post

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("car")

type Post struct {
	url    string
	client *http.Client
}

func NewPost(addr string) *Post {
	url := fmt.Sprintf("http://%s/block", addr)

	client := &http.Client{}

	return &Post{url: url, client: client}
}

type RootBlock struct {
	Root  string `json:"root"`
	Block []byte `json:"block"`
}

func (p *Post) PostRootBlock(root string, block []byte) error {
	rb := RootBlock{
		Root:  root,
		Block: block,
	}

	if err := verify(rb); err != nil {
		return err
	}

	body, err := json.Marshal(&rb)
	if err != nil {
		return err
	}

	resp, err := p.client.Post(p.url, "application/json", bytes.NewBuffer(body))
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

	log.Debugw("post success", "root", root)

	return nil
}

func verify(rb RootBlock) error {
	root, err := cid.Parse(rb.Root)
	if err != nil {
		return err
	}

	new, err := root.Prefix().Sum(rb.Block)
	if err != nil {
		return err
	}

	if !new.Equals(root) {
		return fmt.Errorf("cid not match, %s!=%s", root, new)
	}

	return nil
}
