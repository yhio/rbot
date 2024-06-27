CREATE TABLE IF NOT EXISTS Deals (
    deal_id INT NOT NULL,
    payload_cid TEXT NOT NULL,
    client TEXT,
    provider TEXT,
    start_epoch INT,
    end_epoch INT,

    indexer_result TEXT,
    fetch_result TEXT,
    err_msg TEXT,
    last_update DateTime,
  
    PRIMARY KEY(deal_id)
);