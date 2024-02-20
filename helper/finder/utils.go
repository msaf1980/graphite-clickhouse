package finder

import (
	"time"

	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/metrics"
)

func TargetKey(from, until int64, target, ttl string) string {
	return time.Unix(from, 0).Format("2006-01-02") + ";" + time.Unix(until, 0).Format("2006-01-02") + ";" + target + ";ttl=" + ttl
}

func GetCacheTimeout(now time.Time, from, until int64, cacheConfig *config.CacheConfig) (int32, string, *metrics.CacheMetric) {
	if cacheConfig.ShortDuration == 0 {
		return cacheConfig.DefaultTimeoutSec, cacheConfig.DefaultTimeoutStr, metrics.DefaultCacheMetrics
	}
	duration := time.Second * time.Duration(until-from)
	if duration > cacheConfig.ShortDuration || now.Unix()-until > cacheConfig.ShortUntilOffsetSec {
		return cacheConfig.DefaultTimeoutSec, cacheConfig.DefaultTimeoutStr, metrics.DefaultCacheMetrics
	}
	// short cache ttl
	return cacheConfig.ShortTimeoutSec, cacheConfig.ShortTimeoutStr, metrics.ShortCacheMetrics
}
