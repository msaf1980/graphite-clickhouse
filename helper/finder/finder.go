package finder

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/finder"
	"github.com/lomik/graphite-clickhouse/helper/utils"
	"github.com/lomik/graphite-clickhouse/limiter"
	"github.com/lomik/graphite-clickhouse/metrics"
	"github.com/lomik/graphite-clickhouse/render/data"
	"go.uber.org/zap"
)

// Finder try to fetch cached finder queries
func FinderCached(config *config.Config, ts time.Time, fetchRequests data.MultiTarget, logger *zap.Logger, metricsLen *int) (cachedFind int, maxCacheTimeoutStr string, err error) {
	var lock sync.RWMutex
	var maxCacheTimeout int32
	errors := make([]error, 0, len(fetchRequests))
	var wg sync.WaitGroup
	for tf, targets := range fetchRequests {
		for i, expr := range targets.List {
			wg.Add(1)
			go func(tf data.TimeFrame, target string, targets *data.Targets, n int) {
				defer wg.Done()

				targets.Cache[n].Timeout, targets.Cache[n].TimeoutStr, targets.Cache[n].M = GetCacheTimeout(ts, tf.From, tf.Until, &config.Common.FindCacheConfig)
				if targets.Cache[n].Timeout > 0 {
					if maxCacheTimeout < targets.Cache[n].Timeout {
						maxCacheTimeout = targets.Cache[n].Timeout
						maxCacheTimeoutStr = targets.Cache[n].TimeoutStr
					}
					targets.Cache[n].TS = utils.TimestampTruncate(ts.Unix(), time.Duration(targets.Cache[n].Timeout)*time.Second)
					targets.Cache[n].Key = TargetKey(tf.From, tf.Until, target, targets.Cache[n].TimeoutStr)
					body, err := config.Common.FindCache.Get(targets.Cache[n].Key)
					if err == nil {
						if len(body) > 0 {
							targets.Cache[n].M.CacheHits.Add(1)
							var f finder.Finder
							if strings.HasPrefix(target, "seriesByTag(") {
								f = finder.NewCachedTags(body)
							} else {
								f = finder.NewCachedIndex(body)
							}

							targets.AM.MergeTarget(f.(finder.Result), target, false)
							lock.Lock()
							amLen := targets.AM.Len()
							*metricsLen += amLen
							lock.Unlock()
							targets.Cache[n].Cached = true

							logger.Info("finder", zap.String("get_cache", targets.Cache[n].Key), zap.Time("timestamp_cached", time.Unix(targets.Cache[n].TS, 0)),
								zap.Int("metrics", amLen), zap.Bool("find_cached", true),
								zap.String("ttl", targets.Cache[n].TimeoutStr),
								zap.Int64("from", tf.From), zap.Int64("until", tf.Until))
						}
						return
					}
				}

			}(tf, expr, targets, i)
		}
	}
	wg.Wait()
	if len(errors) != 0 {
		err = errors[0]
		return
	}
	for _, targets := range fetchRequests {
		var cached int
		for _, c := range targets.Cache {
			if c.Cached {
				cached++
			}
		}
		cachedFind += cached
		if cached == len(targets.Cache) {
			targets.Cached = true
		}
	}
	return
}

// Find try to fetch finder queries
func Find(config *config.Config, fetchRequests data.MultiTarget, ctx context.Context, logger *zap.Logger, qlimiter limiter.ServerLimiter, metricsLen *int, queueDuration *time.Duration, useCache bool) (maxDuration int64, err error) {
	var (
		wg       sync.WaitGroup
		lock     sync.RWMutex
		entered  int
		limitCtx context.Context
		cancel   context.CancelFunc
	)
	if qlimiter.Enabled() {
		// no reason wait longer than index-timeout
		limitCtx, cancel = context.WithTimeout(ctx, config.ClickHouse.IndexTimeout)
		defer func() {
			for i := 0; i < entered; i++ {
				qlimiter.Leave(limitCtx, "render")
			}
			defer cancel()
		}()
	}

	errors := make([]error, 0, len(fetchRequests))
	for tf, targets := range fetchRequests {
		for i, expr := range targets.List {
			d := tf.Until - tf.From
			if maxDuration < d {
				maxDuration = d
			}
			if targets.Cache[i].Cached {
				continue
			}
			if qlimiter.Enabled() {
				start := time.Now()
				err = qlimiter.Enter(limitCtx, "render")
				*queueDuration += time.Since(start)
				if err != nil {
					lock.Lock()
					errors = append(errors, err)
					lock.Unlock()
					break
				}
				entered++
			}
			wg.Add(1)
			go func(tf data.TimeFrame, target string, targets *data.Targets, n int) {
				defer wg.Done()

				var fndResult finder.Result
				var err error

				// Search in small index table first
				var stat finder.FinderStat
				fStart := time.Now()
				fndResult, err = finder.Find(config, ctx, target, tf.From, tf.Until, &stat)
				d := time.Since(fStart).Milliseconds()
				if err != nil {
					metrics.SendQueryReadByTable(stat.Table, tf.From, tf.Until, d, 0, 0, stat.ChReadRows, stat.ChReadBytes, true)
					logger.Error("find", zap.Error(err))
					lock.Lock()
					errors = append(errors, err)
					lock.Unlock()
					return
				}
				body := targets.AM.MergeTarget(fndResult, target, useCache)
				cacheTimeout := targets.Cache[n].Timeout
				if useCache && cacheTimeout > 0 {
					cacheTimeoutStr := targets.Cache[n].TimeoutStr
					key := targets.Cache[n].Key
					targets.Cache[n].M.CacheMisses.Add(1)
					config.Common.FindCache.Set(key, body, cacheTimeout)
					logger.Info("finder", zap.String("set_cache", key), zap.Time("timestamp_cached", time.Unix(targets.Cache[n].TS, 0)),
						zap.Int("metrics", targets.AM.Len()), zap.Bool("find_cached", false),
						zap.String("ttl", cacheTimeoutStr),
						zap.Int64("from", tf.From), zap.Int64("until", tf.Until))
				}
				lock.Lock()
				rows := targets.AM.Len()
				lock.Unlock()
				*metricsLen += rows
				metrics.SendQueryReadByTable(stat.Table, tf.From, tf.Until, d, int64(rows), stat.ReadBytes, stat.ChReadRows, stat.ChReadBytes, false)
			}(tf, expr, targets, i)
		}
	}
	wg.Wait()
	if len(errors) != 0 {
		err = errors[0]
	}
	return
}
