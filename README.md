# rbot
此分支 rbot 读取 rootblock 并上传到 retrieve-server
## 从 boost 读取 rootblock
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

## 从七牛云读取 rootblock
1. 启动 rbot 增加环境变量：export QINIU=./qiniu.json
2. ./rbot car ./hpgp.json

## 从 minio 读取 rootblock
1. 启动 rbot 增加环境变量：export MINIO=./minio.json
2. ./rbot car ./hpgp.json

## 从文件系统读取 rootblock
1. 启动 rbot 增加环境变量：export FILE_PATH=/path/to/car
2. ./rbot car ./hpgp.json

