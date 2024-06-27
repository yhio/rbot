package main

import (
	"context"
	"fmt"
	"net/http"

	"github.com/filecoin-project/go-jsonrpc"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/client"
	"github.com/filecoin-project/lotus/api/v1api"
	cliutil "github.com/filecoin-project/lotus/cli/util"
)

type httpHead struct {
	addr   string
	header http.Header
}

func getFullNodeAPIV1(ctx context.Context, ainfoCfg []string) (v1api.FullNode, jsonrpc.ClientCloser, error) {
	var httpHeads []httpHead
	version := "v1"
	{
		if len(ainfoCfg) == 0 {
			return nil, nil, fmt.Errorf("could not get API info: ainfoCfg is empty")
		}
		for _, i := range ainfoCfg {
			ainfo := cliutil.ParseApiInfo(i)
			addr, err := ainfo.DialArgs(version)
			if err != nil {
				return nil, nil, fmt.Errorf("could not get DialArgs: %w", err)
			}
			httpHeads = append(httpHeads, httpHead{addr: addr, header: ainfo.AuthHeader()})
		}
	}

	var fullNodes []api.FullNode
	var closers []jsonrpc.ClientCloser

	for _, head := range httpHeads {
		v1api, closer, err := client.NewFullNodeRPCV1(ctx, head.addr, head.header)
		if err != nil {
			log.Warnf("Not able to establish connection to node with addr: %s, Reason: %s", head.addr, err.Error())
			continue
		}
		log.Infow("connected to lotus", "addr", head.addr)
		fullNodes = append(fullNodes, v1api)
		closers = append(closers, closer)
	}

	if len(httpHeads) > 1 && len(fullNodes) < 2 {
		return nil, nil, fmt.Errorf("not able to establish connection to more than a single node")
	}

	finalCloser := func() {
		for _, c := range closers {
			c()
		}
	}

	var v1API api.FullNodeStruct
	cliutil.FullNodeProxy(fullNodes, &v1API)

	v, err := v1API.Version(ctx)
	if err != nil {
		return nil, nil, err
	}
	if !v.APIVersion.EqMajorMinor(api.FullAPIVersion1) {
		return nil, nil, fmt.Errorf("remote API version didn't match (expected %s, remote %s)", api.FullAPIVersion1, v.APIVersion)
	}

	return &v1API, finalCloser, nil
}
