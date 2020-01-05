package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sort"

	"github.com/jedib0t/go-pretty/table"
	"github.com/jedib0t/go-pretty/text"
)

type rxTx struct {
	Tx int `json:"Tx"`
	Rx int `json:"Rx"`
}

// LogRecord json message
type LogRecord struct {
	ConnID int `json:"conn_id"`
	rxTx
}

func (r *rxTx) add(a rxTx) {
	r.Tx += a.Tx
	r.Rx += a.Rx
}

func drawTable(t string, lrs []LogRecord, topN int) {
	tb := make([]table.Row, topN, topN)

	for i := 0; i < topN; i++ {
		tb[i] = []interface{}{lrs[i].ConnID, lrs[i].Rx, lrs[i].Tx}
	}

	// Populate and render table
	ttx := table.NewWriter()
	ttx.SetAutoIndex(true)
	ttx.SetTitle(t)
	ttx.Style().Title.Align = text.AlignCenter
	ttx.AppendHeader(table.Row{"connId", "RX", "TX"})
	ttx.AppendRows(tb)
	fmt.Println(ttx.Render())
}

func main() {
	path := "hydra.log"
	file, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var rec = new(LogRecord)
	// make map with session as key to summarize traffic per session
	m := make(map[int]rxTx)
	// read file line by line and decode json into struct
	for scanner.Scan() {
		err := json.Unmarshal(scanner.Bytes(), rec)
		if err != nil {
			panic(err)
		}
		// Add record to map if session key does not exist or add value otherwise.
		if _, ok := m[rec.ConnID]; !ok {
			m[rec.ConnID] = rec.rxTx
		} else {
			temp := m[rec.ConnID]
			temp.add(rec.rxTx)
			m[rec.ConnID] = temp
		}
	}
	// Check for non-EOF error
	if err := scanner.Err(); err != nil {
		fmt.Println(err)
	}
	// copy map to slice. We don't have duplicated sessions anymore
	lrs := make([]LogRecord, 0, len(m))
	for k, v := range m {
		lrs = append(lrs, LogRecord{ConnID: k, rxTx: v})
	}

	// Sort by TX
	sort.Slice(lrs, func(i, j int) bool {
		return lrs[i].Tx > lrs[j].Tx
	})
	drawTable("By TX", lrs, 5)

	// Sort by RX
	sort.Slice(lrs, func(i, j int) bool {
		return lrs[i].Rx > lrs[j].Rx
	})
	drawTable("By RX", lrs, 5)
}
