package main

import (
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/urfave/cli/v2"
)

var carCmd = &cli.Command{
	Name:        "car",
	Usage:       "car <carInfo file path>",
	UsageText:   "rbot car <carInfo file path>",
	Description: "fetch root block from qiniu and post to retrieve server",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "connect",
			Value: "127.0.0.1:5678",
		},
		&cli.IntFlag{
			Name:  "parallel",
			Value: 100,
		},
	},
	Action: func(cctx *cli.Context) error {
		path := cctx.Args().Get(0)
		if path == "" {
			return fmt.Errorf("path is required")
		}

		url := fmt.Sprintf("http://%s/car?path=%s&parallel=%d", cctx.String("connect"), url.QueryEscape(path), cctx.Int("parallel"))
		resp, err := http.Get(url)
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
	},
}
