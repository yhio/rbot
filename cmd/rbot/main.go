package main

import (
	"net/http"
	"os"
	"time"

	_ "net/http/pprof"

	"contrib.go.opencensus.io/exporter/prometheus"
	cliutil "github.com/filecoin-project/lotus/cli/util"
	"github.com/gh-efforts/rbot/backfill"
	"github.com/gh-efforts/rbot/build"
	"github.com/gh-efforts/rbot/metrics"
	"github.com/gh-efforts/rbot/onchain"
	"github.com/gh-efforts/rbot/repo"
	"github.com/gh-efforts/rbot/retrieve"
	"github.com/gh-efforts/rbot/web"

	logging "github.com/ipfs/go-log/v2"
	"github.com/urfave/cli/v2"
	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
)

var log = logging.Logger("main")

func main() {
	local := []*cli.Command{
		initCmd,
		runCmd,
		retrieveCmd,
		backfillCmd,
		pprofCmd,
	}

	app := &cli.App{
		Name:     "rbot",
		Usage:    "retrieval bot",
		Version:  build.UserVersion(),
		Commands: local,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "repo",
				Value: "~/.rbot",
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Errorf("%+v", err)
	}
}

var runCmd = &cli.Command{
	Name: "run",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "listen",
			Value: "0.0.0.0:5678",
		},
		&cli.BoolFlag{
			Name:  "debug",
			Value: false,
		},
	},
	Action: func(cctx *cli.Context) error {
		setLog(cctx.Bool("debug"))

		log.Info("starting retrieval bot ...")

		ctx := cliutil.ReqContext(cctx)

		exporter, err := prometheus.NewExporter(prometheus.Options{
			Namespace: "rbot",
		})
		if err != nil {
			return err
		}

		ctx, _ = tag.New(ctx,
			tag.Insert(metrics.Version, build.BuildVersion),
			tag.Insert(metrics.Commit, build.CurrentCommit),
		)
		if err := view.Register(
			metrics.Views...,
		); err != nil {
			return err
		}
		stats.Record(ctx, metrics.Info.M(1))

		err = repo.Init(ctx, cctx.String("repo"))
		if err != nil {
			return err
		}
		r, err := repo.New(cctx.String("repo"))
		if err != nil {
			return err
		}

		fullnode, close, err := getFullNodeAPIV1(ctx, r.Conf.Lotus)
		if err != nil {
			return err
		}
		defer close()

		oc, err := onchain.New(ctx, r, fullnode)
		if err != nil {
			return err
		}
		go oc.SubscribeDealActivatedEvent(ctx)

		rt, err := retrieve.New(ctx, r, fullnode)
		if err != nil {
			return err
		}
		go rt.Run(ctx)

		listen := cctx.String("listen")
		log.Infow("rbot server", "listen", listen)

		http.Handle("/metrics", exporter)
		http.HandleFunc("/", web.New(r).Index)
		http.HandleFunc("/backfill", backfill.New(r).Fill)
		http.HandleFunc("/retrieve", rt.ManualRetrieve)

		server := &http.Server{
			Addr: listen,
		}

		go func() {
			<-ctx.Done()
			time.Sleep(time.Millisecond * 100)
			log.Info("closed rbot server")
			server.Shutdown(ctx)
		}()

		return server.ListenAndServe()
	},
}

var initCmd = &cli.Command{
	Name:  "init",
	Usage: "init repo",
	Action: func(cctx *cli.Context) error {
		ctx := cliutil.ReqContext(cctx)
		logging.SetLogLevel("repo", "INFO")
		return repo.Init(ctx, cctx.String("repo"))
	},
}

func setLog(debug bool) {
	level := "INFO"
	if debug {
		level = "DEBUG"
	}

	logging.SetLogLevel("main", level)
	logging.SetLogLevel("repo", level)
	logging.SetLogLevel("onchain", level)
	logging.SetLogLevel("retrieve", level)
	logging.SetLogLevel("web", level)
	logging.SetLogLevel("backfill", level)
}
