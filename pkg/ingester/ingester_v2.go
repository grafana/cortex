package ingester

import (
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/ring"
	cortex_tsdb "github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/validation"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/tsdb"
	lbls "github.com/prometheus/prometheus/tsdb/labels"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/runutil"
	"github.com/thanos-io/thanos/pkg/shipper"
	"github.com/weaveworks/common/user"
	"golang.org/x/net/context"
	old_ctx "golang.org/x/net/context"
)

// TSDBState holds data structures used by the TSDB storage engine
type TSDBState struct {
	dbs    map[string]*tsdb.DB // tsdb sharded by userID
	bucket objstore.Bucket

	// Keeps count of in-flight requests
	inflightWriteReqs sync.WaitGroup

	// Used to run only once operations at shutdown, during the blocks/wal
	// transferring to a joining ingester
	transferOnce sync.Once
}

// NewV2 returns a new Ingester that uses prometheus block storage instead of chunk storage
func NewV2(cfg Config, clientConfig client.Config, limits *validation.Overrides, chunkStore ChunkStore, registerer prometheus.Registerer) (*Ingester, error) {
	bucketClient, err := cortex_tsdb.NewBucketClient(context.Background(), cfg.TSDBConfig, "cortex", util.Logger)
	if err != nil {
		return nil, err
	}

	i := &Ingester{
		cfg:          cfg,
		clientConfig: clientConfig,
		metrics:      newIngesterMetrics(registerer),
		limits:       limits,
		chunkStore:   chunkStore,
		quit:         make(chan struct{}),

		TSDBState: TSDBState{
			dbs:    make(map[string]*tsdb.DB),
			bucket: bucketClient,
		},
	}

	i.lifecycler, err = ring.NewLifecycler(cfg.LifecyclerConfig, i, "ingester")
	if err != nil {
		return nil, err
	}

	// Init the limter and instantiate the user states which depend on it
	i.limiter = NewSeriesLimiter(limits, i.lifecycler, cfg.LifecyclerConfig.RingConfig.ReplicationFactor, cfg.ShardByAllLabels)
	i.userStates = newUserStates(i.limiter, cfg)

	// Now that user states have been created, we can start the lifecycler
	i.lifecycler.Start()

	return i, nil
}

// v2Push adds metrics to a block
func (i *Ingester) v2Push(ctx old_ctx.Context, req *client.WriteRequest) (*client.WriteResponse, error) {
	userID, err := user.ExtractOrgID(ctx)
	if err != nil {
		return nil, fmt.Errorf("no user id")
	}

	db, err := i.getOrCreateTSDB(userID)
	if err != nil {
		return nil, err
	}

	// Ensure the ingester shutdown procedure hasn't started
	i.userStatesMtx.RLock()

	if i.stopped {
		i.userStatesMtx.RUnlock()
		return nil, fmt.Errorf("ingester stopping")
	}

	// Keep track of in-flight requests, in order to safely start blocks transfer
	// (at shutdown) only once all in-flight write requests have completed
	i.TSDBState.inflightWriteReqs.Add(1)
	i.userStatesMtx.RUnlock()
	defer i.TSDBState.inflightWriteReqs.Done()

	// Walk the samples, appending them to the users database
	app := db.Appender()
	for _, ts := range req.Timeseries {
		// Convert labels to the type expected by TSDB
		lset := cortex_tsdb.FromLabelAdaptersToLabels(ts.Labels)

		for _, s := range ts.Samples {
			if _, err := app.Add(lset, s.TimestampMs, s.Value); err != nil {
				if err := app.Rollback(); err != nil {
					level.Warn(util.Logger).Log("msg", "failed to rollback on error", "userID", userID, "err", err)
				}
				return nil, err
			}
		}
	}
	if err := app.Commit(); err != nil {
		return nil, err
	}

	client.ReuseSlice(req.Timeseries)

	return &client.WriteResponse{}, nil
}

func (i *Ingester) v2Query(ctx old_ctx.Context, req *client.QueryRequest) (*client.QueryResponse, error) {
	userID, err := user.ExtractOrgID(ctx)
	if err != nil {
		return nil, err
	}

	from, through, matchers, err := client.FromQueryRequest(req)
	if err != nil {
		return nil, err
	}

	i.metrics.queries.Inc()

	db, err := i.getOrCreateTSDB(userID)
	if err != nil {
		return nil, fmt.Errorf("failed to find/create user db: %v", err)
	}

	q, err := db.Querier(int64(from), int64(through))
	if err != nil {
		return nil, err
	}
	defer q.Close()

	// two different versions of the labels package are being used, converting matchers must be done
	var converted []lbls.Matcher
	for _, m := range matchers {
		switch m.Type {
		case labels.MatchEqual:
			converted = append(converted, lbls.NewEqualMatcher(m.Name, m.Value))
		case labels.MatchNotEqual:
			converted = append(converted, lbls.Not(lbls.NewEqualMatcher(m.Name, m.Value)))
		case labels.MatchRegexp:
			rm, err := lbls.NewRegexpMatcher(m.Name, "^(?:"+m.Value+")$")
			if err != nil {
				return nil, err
			}
			converted = append(converted, rm)
		case labels.MatchNotRegexp:
			rm, err := lbls.NewRegexpMatcher(m.Name, "^(?:"+m.Value+")$")
			if err != nil {
				return nil, err
			}
			converted = append(converted, lbls.Not(rm))
		}
	}
	ss, err := q.Select(converted...)
	if err != nil {
		return nil, err
	}

	result := &client.QueryResponse{}
	for ss.Next() {
		series := ss.At()

		// convert labels to LabelAdapter
		var adapters []client.LabelAdapter
		for _, l := range series.Labels() {
			adapters = append(adapters, client.LabelAdapter(l))
		}
		ts := client.TimeSeries{
			Labels: adapters,
		}

		it := series.Iterator()
		for it.Next() {
			t, v := it.At()
			ts.Samples = append(ts.Samples, client.Sample{Value: v, TimestampMs: t})
		}

		result.Timeseries = append(result.Timeseries, ts)
	}

	return result, ss.Err()
}

func (i *Ingester) v2LabelValues(ctx old_ctx.Context, req *client.LabelValuesRequest) (*client.LabelValuesResponse, error) {
	userID, err := user.ExtractOrgID(ctx)
	if err != nil {
		return nil, err
	}

	db, err := i.getOrCreateTSDB(userID)
	if err != nil {
		return nil, fmt.Errorf("failed to find/create user db: %v", err)
	}

	through := time.Now()
	from := through.Add(-i.cfg.TSDBConfig.Retention)
	q, err := db.Querier(from.Unix()*1000, through.Unix()*1000)
	if err != nil {
		return nil, err
	}
	defer q.Close()

	vals, err := q.LabelValues(req.LabelName)
	if err != nil {
		return nil, err
	}

	return &client.LabelValuesResponse{
		LabelValues: vals,
	}, nil
}

func (i *Ingester) v2LabelNames(ctx old_ctx.Context, req *client.LabelNamesRequest) (*client.LabelNamesResponse, error) {
	userID, err := user.ExtractOrgID(ctx)
	if err != nil {
		return nil, err
	}

	db, err := i.getOrCreateTSDB(userID)
	if err != nil {
		return nil, fmt.Errorf("failed to find/create user db: %v", err)
	}

	through := time.Now()
	from := through.Add(-i.cfg.TSDBConfig.Retention)
	q, err := db.Querier(from.Unix()*1000, through.Unix()*1000)
	if err != nil {
		return nil, err
	}
	defer q.Close()

	names, err := q.LabelNames()
	if err != nil {
		return nil, err
	}

	return &client.LabelNamesResponse{
		LabelNames: names,
	}, nil
}

func (i *Ingester) getTSDB(userID string) *tsdb.DB {
	i.userStatesMtx.RLock()
	defer i.userStatesMtx.RUnlock()
	db, _ := i.TSDBState.dbs[userID]
	return db
}

func (i *Ingester) getOrCreateTSDB(userID string) (*tsdb.DB, error) {
	db := i.getTSDB(userID)
	if db == nil {
		i.userStatesMtx.Lock()
		defer i.userStatesMtx.Unlock()

		// Check again for DB in the event it was created in-between locks
		var ok bool
		db, ok = i.TSDBState.dbs[userID]
		if !ok {

			udir := i.userDir(userID)

			// Create a new user database
			var err error
			db, err = tsdb.Open(udir, util.Logger, nil, &tsdb.Options{
				RetentionDuration: uint64(i.cfg.TSDBConfig.Retention / time.Millisecond),
				BlockRanges:       i.cfg.TSDBConfig.BlockRanges.ToMillisecondRanges(),
				NoLockfile:        true,
			})
			if err != nil {
				return nil, err
			}

			// Thanos shipper requires at least 1 external label to be set. For this reason,
			// we set the tenant ID as external label and we'll filter it out when reading
			// the series from the storage.
			l := lbls.Labels{
				{
					Name:  cortex_tsdb.TenantIDExternalLabel,
					Value: userID,
				},
			}

			// Create a new shipper for this database
			s := shipper.New(util.Logger, nil, udir, &Bucket{userID, i.TSDBState.bucket}, func() lbls.Labels { return l }, metadata.ReceiveSource)
			i.done.Add(1)
			go func() {
				defer i.done.Done()
				runutil.Repeat(i.cfg.TSDBConfig.ShipInterval, i.quit, func() error {
					if uploaded, err := s.Sync(context.Background()); err != nil {
						level.Warn(util.Logger).Log("err", err, "uploaded", uploaded)
					}
					return nil
				})
			}()

			i.TSDBState.dbs[userID] = db
		}
	}

	return db, nil
}

func (i *Ingester) userDir(userID string) string { return filepath.Join(i.cfg.TSDBConfig.Dir, userID) }

func (i *Ingester) closeAllTSDB() {
	i.userStatesMtx.Lock()

	wg := &sync.WaitGroup{}
	wg.Add(len(i.TSDBState.dbs))

	// Concurrently close all users TSDB
	for userID, db := range i.TSDBState.dbs {
		userID := userID

		go func(db *tsdb.DB) {
			defer wg.Done()

			if err := db.Close(); err != nil {
				level.Warn(util.Logger).Log("msg", "unable to close TSDB", "err", err, "user", userID)
				return
			}

			// Now that the TSDB has been closed, we should remove it from the
			// set of open ones.
			i.userStatesMtx.Lock()
			delete(i.TSDBState.dbs, userID)
			i.userStatesMtx.Unlock()
		}(db)
	}

	// Wait until all Close() completed
	i.userStatesMtx.Unlock()
	wg.Wait()
}
