package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/rawkv"
	"io/ioutil"
	"os"
	"strings"
	"time"

	"github.com/pingcap/log"
	"github.com/tikv/client-go/v2/txnkv"
	"go.uber.org/zap"
)

var (
	sourceTiKV      = flag.String("source-tikv", "", "source tikv address")
	sourceMode      = flag.String("source-mode", "txn", "source tikv mode: txn or raw")
	targetTiKV      = flag.String("target-tikv", "", "target tikv address")
	targetMode      = flag.String("target-mode", "txn", "target tikv mode: txn or raw")
	stateFile       = flag.String("state-file", "state.json", "file to record copy state for resume")
	batchSize       = flag.Int("batch-size", 10000, "batch size")
	checkpointSize  = flag.Int("checkpoint-size", 1000000, "checkpoint size")
	backupStateFile = ""
)

func createScanBuffer() ([][]byte, [][]byte) {
	all := make([][]byte, 2**batchSize)
	return all[0:*batchSize], all[*batchSize:]
}

type client struct {
	txn *txnkv.Client
	raw *rawkv.Client
}

func createClient(ctx context.Context, addr, mode string) (*client, error) {
	pdAddrs := strings.Split(addr, ",")
	switch strings.ToLower(mode) {
	case "txn":
		if c, err := txnkv.NewClient(pdAddrs); err != nil {
			return nil, err
		} else {
			return &client{txn: c}, nil
		}
	case "raw":
		if c, err := rawkv.NewClient(ctx, pdAddrs, config.DefaultConfig().Security); err != nil {
			return nil, err
		} else {
			return &client{raw: c}, nil
		}
	default:
		log.Fatal("Invalid TiKV mode", zap.String("mode", mode))
	}
	return nil, errors.New("Invalid TiKV mode:" + mode)
}

func (c *client) txnScan(ctx context.Context, startKey []byte) ([][]byte, [][]byte, error) {
	records := 0
	tx, err := c.txn.Begin()
	if err != nil {
		return nil, nil, err
	}
	it, err := tx.Iter(startKey, nil)
	if err != nil {
		return nil, nil, err
	}
	defer it.Close()

	keys, values := createScanBuffer()
	for it.Valid() && records < *batchSize {
		keys[records] = it.Key()
		values[records] = it.Value()
		records += 1
		it.Next()
	}
	return keys[0:records], values[0:records], nil
}

func (c *client) scan(ctx context.Context, startKey []byte) ([][]byte, [][]byte, error) {
	if c.txn != nil {
		return c.txnScan(ctx, startKey)
	}
	return c.raw.Scan(ctx, startKey, nil, *batchSize)
}

func (c *client) write(ctx context.Context, keys, values [][]byte) error {
	if c.txn != nil {
		tx, err := c.txn.Begin()
		if err != nil {
			return err
		}
		for i := 0; i < len(keys); i++ {
			tx.Set(keys[i], values[i])
		}
		return tx.Commit(ctx)
	}
	if len(keys) > 0 {
		return c.raw.BatchPut(ctx, keys, values)
	}
	return nil
}

type CopyState struct {
	StartKey []byte `json:"start_key,omitempty"`
	Finished uint64 `json:"finished"`
}

func runBatch(ctx context.Context, src, target *client, state *CopyState) (int, error) {
	log.Debug("run batch", zap.ByteString("start-key", state.StartKey))

	keys, values, err := src.scan(ctx, state.StartKey)
	if err != nil {
		return 0, err
	}
	if err = target.write(ctx, keys, values); err != nil {
		return 0, err
	}
	if len(keys) > 0 {
		lastKey := keys[len(keys)-1]
		state.StartKey = kv.NextKey(lastKey)
	}

	return len(keys), nil
}

func run(ctx context.Context, src, target *client, state *CopyState) (int, error) {
	records := 0
	for {
		if batch, err := runBatch(ctx, src, target, state); err != nil {
			return records, err
		} else {
			if batch == 0 {
				break
			}
			records += batch
		}
		if records > *checkpointSize {
			break
		}
	}
	state.Finished += uint64(records)
	return records, nil
}

func main() {
	flag.Parse()
	backupStateFile = *stateFile + ".bak"
	ctx := context.Background()

	src, err := createClient(ctx, *sourceTiKV, *sourceMode)
	if err != nil {
		log.Fatal("Failed to connect to source tikv cluster", zap.String("source cluster", *sourceTiKV), zap.Error(err))
	}
	target, err := createClient(ctx, *targetTiKV, *targetMode)
	if err != nil {
		log.Fatal("Failed to connect to target tikv cluster", zap.String("target cluster", *targetTiKV), zap.Error(err))
	}

	state := &CopyState{}

	if file, err := os.ReadFile(*stateFile); err != nil && !os.IsNotExist(err) {
		log.Fatal("failed to read state file", zap.Error(err))
	} else {
		if len(file) > 0 {
			if err = json.Unmarshal(file, state); err != nil {
				log.Fatal("failed to unmarshal state file", zap.Error(err))
			}
		}
	}

	if state.Finished > 0 {
		log.Info("Resume copying keys", zap.ByteString("start", state.StartKey), zap.Uint64("finished", state.Finished))
	} else {
		log.Info("Start copying key from beginning")
	}

	startFrom := state.Finished
	backoff := time.Second

	for {
		if records, err := run(ctx, src, target, state); err != nil {
			log.Info("Failed to migrate some records, sleep before next retry", zap.Duration("backoff", backoff), zap.Error(err))
			time.Sleep(backoff)
			if backoff < time.Minute {
				backoff *= 2
			}
		} else {
			if records == 0 {
				if state.Finished == startFrom {
					log.Info("No record to migrate", zap.Uint64("total", state.Finished))
				} else {
					log.Info("No more records to migrate", zap.Uint64("total", state.Finished), zap.Uint64("new", state.Finished-startFrom))
				}
				break
			}
			backoff = time.Second
			bits, _ := json.Marshal(state)

			// Backup old state and ignore error for simplicity
			if old, err := ioutil.ReadFile(*stateFile); err == nil {
				ioutil.WriteFile(backupStateFile, old, 0644)
			}
			if err := os.WriteFile(*stateFile, bits, 0644); err != nil {
				log.Info("Failed to store state file", zap.Error(err))
			}
		}
	}
}
