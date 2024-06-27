package repo

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"

	logging "github.com/ipfs/go-log/v2"
	"github.com/mitchellh/go-homedir"
)

var log = logging.Logger("repo")

const (
	fsDB     = "rbot.db"
	fsConfig = "config.json"
)

type Repo struct {
	path string
	DB   *sql.DB
	Conf *Config
}

func New(path string) (*Repo, error) {
	path, err := homedir.Expand(path)
	if err != nil {
		return nil, err
	}

	conf, err := loadConfig(filepath.Join(path, fsConfig))
	if err != nil {
		return nil, err
	}

	db, err := openDB(filepath.Join(path, fsDB))
	if err != nil {
		return nil, err
	}

	return &Repo{
		path: path,
		DB:   db,
		Conf: conf,
	}, nil
}

func Init(ctx context.Context, path string) error {
	path, err := homedir.Expand(path)
	if err != nil {
		return err
	}

	exist, err := exists(path)
	if err != nil {
		return err
	}
	if exist {
		log.Warnf("repo: %s alreadt exist", path)
		return nil
	}

	log.Infof("Initializing repo at '%s'", path)

	err = os.MkdirAll(path, 0755)
	if err != nil && !os.IsExist(err) {
		return err
	}

	db, err := openDB(filepath.Join(path, fsDB))
	if err != nil {
		return err
	}

	err = createDB(ctx, db)
	if err != nil {
		return err
	}

	err = initConfig(filepath.Join(path, fsConfig))
	if err != nil {
		return err
	}

	return nil
}

func exists(path string) (bool, error) {
	_, err := os.Stat(filepath.Join(path, fsDB))
	notexist := os.IsNotExist(err)
	if notexist {
		err = nil

		_, err = os.Stat(filepath.Join(path, fsConfig))
		notexist = os.IsNotExist(err)
		if notexist {
			err = nil
		}
	}

	return !notexist, err
}
