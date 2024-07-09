package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/gh-efforts/rbot/retrieve"
	"github.com/urfave/cli/v2"
)

var retrieveCmd = &cli.Command{
	Name:  "retrieve",
	Usage: "manual retrieve",
	Flags: []cli.Flag{
		&cli.StringSliceFlag{
			Name: "provider",
		},
		&cli.IntFlag{
			Name:  "limit",
			Value: 100,
		},
		&cli.IntFlag{
			Name:  "parallel",
			Value: 10,
		},
		&cli.StringFlag{
			Name:  "connect",
			Value: "127.0.0.1:5678",
		},
	},
	Action: func(cctx *cli.Context) error {
		mp := retrieve.ManualParam{
			Providers: cctx.StringSlice("provider"),
			Limit:     cctx.Int("limit"),
			Parallel:  cctx.Int("parallel"),
		}
		body, err := json.Marshal(&mp)
		if err != nil {
			return err
		}

		url := fmt.Sprintf("http://%s/retrieve", cctx.String("connect"))
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
	},
}
