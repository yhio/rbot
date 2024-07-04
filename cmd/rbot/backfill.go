package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/gh-efforts/rbot/backfill"
	"github.com/urfave/cli/v2"
)

var backfillCmd = &cli.Command{
	Name:  "backfill",
	Usage: "backfill deals",
	Flags: []cli.Flag{
		&cli.StringSliceFlag{
			Name: "providers",
		},
		&cli.IntFlag{
			Name: "start-epoch",
		},
		&cli.StringFlag{
			Name:  "connect",
			Value: "127.0.0.1:5678",
		},
	},
	Action: func(cctx *cli.Context) error {
		f := backfill.Filter{
			Providers:  cctx.StringSlice("providers"),
			StartEpoch: cctx.Int("start-epoch"),
		}
		body, err := json.Marshal(&f)
		if err != nil {
			return err
		}

		url := fmt.Sprintf("http://%s/backfill", cctx.String("connect"))
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
