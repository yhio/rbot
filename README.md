# rbot
retrieval bot
- Monitor the deal-activated(id, client, provider) events of the specified SP on chain.
- Get payloadCid(label) through lotus api: StateMarketStorageDeal.
- Save Deal(deal_id, payload_cid, client, provider) to DB(sqlite3).
- Random select the limit Deal from the DB regularly.
- Lookup indexer(cid.contact) to find SP address of payloadCid.
- Fetch RootCid(DagScopeBlock) from SP.
- Update Deal(indexer_result, fetch_result, last_update) to DB.
- A web to veiw data.
- Support backfill Deal from [StateMarketDeals](https://marketdeals.s3.amazonaws.com/StateMarketDeals.json.zst).
- Support manual trigger retrieve.