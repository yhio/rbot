package web

import (
	"database/sql"
	"embed"
	"html/template"
	"net/http"
	"strconv"

	"github.com/gh-efforts/rbot/repo"
	logging "github.com/ipfs/go-log/v2"
)

//go:embed templates/*
var tmplFS embed.FS

var log = logging.Logger("web")

var tmpl = template.Must(template.ParseFS(tmplFS, "templates/index.html"))

type Web struct {
	repo *repo.Repo
}

type Deal struct {
	DealID        int
	PayloadCID    string
	Client        string
	Provider      string
	StartEpoch    int
	EndEpoch      int
	IndexerResult string
	FetchResult   string
	ErrMsg        string
	LastUpdate    string
}

type PageData struct {
	Deals    []Deal
	Client   string
	Provider string
	PrevPage int
	NextPage int
}

func New(repo *repo.Repo) *Web {
	log.Info("rbot web running...")
	return &Web{
		repo: repo,
	}
}

func (e *Web) Index(w http.ResponseWriter, r *http.Request) {
	db := e.repo.DB

	client := r.URL.Query().Get("client")
	provider := r.URL.Query().Get("provider")
	page := r.URL.Query().Get("page")
	pageSize := 10
	pageNum, err := strconv.Atoi(page)
	if err != nil || pageNum < 1 {
		pageNum = 1
	}
	offset := (pageNum - 1) * pageSize

	query := "SELECT deal_id, payload_cid, client, provider, start_epoch, end_epoch, indexer_result, fetch_result, err_msg, last_update FROM Deals WHERE 1=1"
	if client != "" {
		query += " AND client LIKE '%" + client + "%'"
	}
	if provider != "" {
		query += " AND provider LIKE '%" + provider + "%'"
	}
	query += " AND last_update IS NOT NULL ORDER BY last_update DESC"
	query += " LIMIT ? OFFSET ?"

	rows, err := db.Query(query, pageSize, offset)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var deals []Deal
	var indexerResult, fetchResult, errMsg, lastUpdate sql.NullString
	for rows.Next() {
		var deal Deal
		err := rows.Scan(&deal.DealID, &deal.PayloadCID, &deal.Client, &deal.Provider, &deal.StartEpoch, &deal.EndEpoch, &indexerResult, &fetchResult, &errMsg, &lastUpdate)
		if err != nil {
			log.Error(err)
			continue
		}
		deal.IndexerResult = indexerResult.String
		deal.FetchResult = fetchResult.String
		deal.ErrMsg = errMsg.String
		deal.LastUpdate = lastUpdate.String
		deals = append(deals, deal)
	}

	nextPage := pageNum + 1
	prevPage := pageNum - 1
	if prevPage < 1 {
		prevPage = 1
	}

	data := PageData{
		Deals:    deals,
		Client:   client,
		Provider: provider,
		PrevPage: prevPage,
		NextPage: nextPage,
	}

	tmpl.Execute(w, data)
}
