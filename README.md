# rbot
1. 配置文件
```json
{
        "serverAddr": "10.122.6.17:9876",
        "providers": {
              "f08412": "/ip4/10.122.4.13/tcp/8023/p2p/12D3KooWPXu7E8t8GQuigggEXmLBaCs9D1vdRJKipayGSWSDSy4G+graphsync",
        },
         "parallel": 10,
        "limit": 100
}
```
2. 下载 [StateMarketDeals](https://marketdeals.s3.amazonaws.com/StateMarketDeals.json.zst) 到 .rbot
3. 筛选指定 SP 的交易并保存到数据库：./rbot backfill --provider f08412 --start-epoch
4. 检索并把 rootblock 上传到 retrieve-server: ./rbot retrieve --provider f08412 --limit 0 --result (TIMEOUT ERR NOTFOUND NOUNSEALED)
