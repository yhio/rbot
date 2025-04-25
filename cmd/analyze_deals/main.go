package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/api"
	logging "github.com/ipfs/go-log/v2"
	"github.com/klauspost/compress/zstd"
)

var log = logging.Logger("analyze-deals")

type ProviderStats struct {
	Count       int
	MinEndEpoch abi.ChainEpoch
	MaxEndEpoch abi.ChainEpoch
}

// Filecoin创世区块时间
var genesisTime = time.Date(2020, 8, 24, 22, 0, 0, 0, time.UTC)

func epochToTime(epoch abi.ChainEpoch) time.Time {
	// 每个epoch约30秒
	duration := time.Duration(epoch) * 30 * time.Second
	return genesisTime.Add(duration)
}

func formatTime(t time.Time) string {
	// 转换为北京时间
	loc, _ := time.LoadLocation("Asia/Shanghai")
	return t.In(loc).Format("2006-01-02 15:04:05")
}

func main() {
	// 解析命令行参数
	dealFile := flag.String("file", "", "StorageMarketDealFile文件路径")
	providersFile := flag.String("providers-file", "", "包含provider列表的文件路径")
	flag.Parse()

	if *dealFile == "" || *providersFile == "" {
		fmt.Println("请提供必要的参数：")
		flag.PrintDefaults()
		os.Exit(1)
	}

	// 从文件读取providers
	providersData, err := os.ReadFile(*providersFile)
	if err != nil {
		log.Fatalf("读取provider文件失败: %v", err)
	}

	// 解析providers
	providers := make(map[string]struct{})
	providerLines := strings.Split(string(providersData), "\n")
	for _, line := range providerLines {
		line = strings.TrimSpace(line)
		if line != "" {
			providers[line] = struct{}{}
		}
	}

	if len(providers) == 0 {
		log.Fatal("没有找到有效的provider")
	}

	// 初始化每个provider的统计信息
	stats := make(map[string]*ProviderStats)
	for p := range providers {
		stats[p] = &ProviderStats{
			MinEndEpoch: 1<<63 - 1,
			MaxEndEpoch: 0,
		}
	}

	// 打开文件
	file, err := os.Open(*dealFile)
	if err != nil {
		log.Fatalf("打开文件失败: %v", err)
	}
	defer file.Close()

	// 创建zstd解压器
	dec, err := zstd.NewReader(file)
	if err != nil {
		log.Fatalf("创建zstd解压器失败: %v", err)
	}
	defer dec.Close()

	// 创建JSON解码器
	decoder := json.NewDecoder(bufio.NewReader(dec))

	// 验证JSON头部
	head, err := decoder.Token()
	if err != nil {
		log.Fatalf("读取JSON头部失败: %v", err)
	}

	if delim, ok := head.(json.Delim); !ok || delim != '{' {
		log.Fatalf("期望JSON头部为{，但得到: %v", delim)
	}

	// 遍历所有交易
	for decoder.More() {
		// 读取交易ID
		_, err := decoder.Token()
		if err != nil {
			log.Fatalf("读取JSON键失败: %v", err)
		}

		// 解析交易数据
		var deal api.MarketDeal
		if err := decoder.Decode(&deal); err != nil {
			log.Fatalf("解析交易数据失败: %v", err)
		}

		provider := deal.Proposal.Provider.String()
		// 检查是否是目标provider
		if _, ok := providers[provider]; !ok {
			continue
		}

		// 更新统计信息
		stat := stats[provider]
		stat.Count++
		if deal.Proposal.EndEpoch < stat.MinEndEpoch {
			stat.MinEndEpoch = deal.Proposal.EndEpoch
		}
		if deal.Proposal.EndEpoch > stat.MaxEndEpoch {
			stat.MaxEndEpoch = deal.Proposal.EndEpoch
		}
	}

	// 输出结果
	fmt.Printf("分析结果:\n")
	fmt.Printf("Provider总数: %d\n", len(providers))
	fmt.Println("\n按Provider统计:")
	fmt.Println("--------------------------------------------------------------------------------------------------------")
	fmt.Printf("%-20s %-10s %-15s %-20s %-15s %-20s\n",
		"Provider",
		"交易数",
		"MinEndEpoch",
		"最早结束时间",
		"MaxEndEpoch",
		"最晚结束时间")
	fmt.Println("--------------------------------------------------------------------------------------------------------")

	for provider, stat := range stats {
		if stat.Count > 0 {
			fmt.Printf("%-20s %-10d %-15d %-20s %-15d %-20s\n",
				provider,
				stat.Count,
				stat.MinEndEpoch,
				formatTime(epochToTime(stat.MinEndEpoch)),
				stat.MaxEndEpoch,
				formatTime(epochToTime(stat.MaxEndEpoch)))
		}
	}
	fmt.Println("--------------------------------------------------------------------------------------------------------")
}
