package main

import (
	"io"
	"net/http"
	"os"

	"github.com/urfave/cli/v2"
)

var pprofCmd = &cli.Command{
	Name: "pprof",
	Subcommands: []*cli.Command{
		pprofGoroutines,
	},
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "connect",
			Value: "127.0.0.1:5678",
		},
	},
}

var pprofGoroutines = &cli.Command{
	Name:  "goroutines",
	Usage: "Get goroutine stacks",
	Action: func(cctx *cli.Context) error {
		connect := cctx.String("connect")
		addr := "http://" + connect + "/debug/pprof/goroutine?debug=2"

		r, err := http.Get(addr)
		if err != nil {
			return err
		}

		if _, err := io.Copy(os.Stdout, r.Body); err != nil {
			return err
		}

		return r.Body.Close()
	},
}
