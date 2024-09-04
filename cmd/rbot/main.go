package main

import (
	"net/http"
	"os"
	"time"

	_ "net/http/pprof"

	cliutil "github.com/filecoin-project/lotus/cli/util"
	"github.com/gh-efforts/rbot/backfill"
	"github.com/gh-efforts/rbot/build"
	"github.com/gh-efforts/rbot/car"
	"github.com/gh-efforts/rbot/post"
	"github.com/gh-efforts/rbot/repo"
	"github.com/gh-efforts/rbot/retrieve"

	logging "github.com/ipfs/go-log/v2"
	"github.com/urfave/cli/v2"
)

var log = logging.Logger("main")

func main() {
	local := []*cli.Command{
		runCmd,
		retrieveCmd,
		backfillCmd,
		carCmd,
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

		err := repo.Init(ctx, cctx.String("repo"))
		if err != nil {
			return err
		}
		r, err := repo.New(cctx.String("repo"))
		if err != nil {
			return err
		}
		post := post.NewPost(r.Conf.ServerAddr)
		rt, err := retrieve.New(ctx, r, post)
		if err != nil {
			return err
		}
		car, err := car.New(ctx, post)
		if err != nil {
			return err
		}

		listen := cctx.String("listen")
		log.Infow("rbot server", "listen", listen)

		http.HandleFunc("POST /backfill", backfill.New(r).Fill)
		http.HandleFunc("POST /retrieve", rt.ManualRetrieve)
		http.HandleFunc("GET /car", car.Fetch)

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

func setLog(debug bool) {
	level := "INFO"
	if debug {
		level = "DEBUG"
	}

	logging.SetLogLevel("main", level)
	logging.SetLogLevel("repo", level)
	logging.SetLogLevel("retrieve", level)
	logging.SetLogLevel("backfill", level)
	logging.SetLogLevel("car", level)
	logging.SetLogLevel("post", level)
}
