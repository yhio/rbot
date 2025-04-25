# Filecoin 交易分析工具

这是一个用于分析Filecoin存储市场交易数据的命令行工具。它可以统计指定存储提供商（Provider）的交易信息，包括交易数量和交易结束时间范围。

## 功能特点

- 支持从zstd压缩的JSON文件中读取交易数据
- 可以指定多个存储提供商进行分析
- 显示每个提供商的交易统计信息：
  - 交易总数
  - 最小结束Epoch（MinEndEpoch）
  - 最大结束Epoch（MaxEndEpoch）
  - 对应的实际时间（基于Filecoin创世区块时间计算）

## 使用方法

1. 准备存储提供商列表文件（例如 `providers.txt`），每行一个提供商ID：
```
f09994134
f09998480
f09997841
...
```

2. 运行分析工具：
```bash
go run main.go -file /path/to/storage-market-deals.json.zst -providers-file providers.txt
```

## 参数说明

- `-file`: StorageMarketDealFile文件路径（必需）
- `-providers-file`: 包含provider列表的文件路径（必需）

## 输出示例

```
分析结果:
Provider总数: 100

按Provider统计:
--------------------------------------------------------------------------------------------------------
Provider             交易数      MinEndEpoch      最早结束时间         MaxEndEpoch      最晚结束时间         
--------------------------------------------------------------------------------------------------------
f09994134            100        123456           2023-01-01 12:34:56   234567           2023-02-01 12:34:56   
f09998480            200        234567           2023-02-01 12:34:56   345678           2023-03-01 12:34:56   
...
--------------------------------------------------------------------------------------------------------
```

## 注意事项

- 时间计算基于Filecoin创世区块时间（2020年8月24日22:00 UTC）
- 每个Epoch约30秒
- 只显示有交易的提供商信息
- 时间已自动转换为北京时间

## 依赖项

- Go 1.16+
- github.com/filecoin-project/go-state-types
- github.com/filecoin-project/lotus/api
- github.com/klauspost/compress/zstd
- github.com/ipfs/go-log/v2 