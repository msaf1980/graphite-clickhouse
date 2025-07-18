package config

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"net"
	"net/url"
	"os"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cactus/go-statsd-client/v5/statsd"
	"github.com/lomik/carbon-clickhouse/helper/config"
	"github.com/msaf1980/go-metrics/graphite"
	"github.com/msaf1980/go-timeutils/duration"
	toml "github.com/pelletier/go-toml"
	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/lomik/zapwriter"

	"github.com/lomik/graphite-clickhouse/cache"
	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
	"github.com/lomik/graphite-clickhouse/helper/date"
	"github.com/lomik/graphite-clickhouse/helper/rollup"
	"github.com/lomik/graphite-clickhouse/limiter"
	"github.com/lomik/graphite-clickhouse/metrics"
)

type SDType uint8

const (
	SDNone  SDType = iota
	SDNginx        // https://github.com/weibocom/nginx-upsync-module
)

var sdTypeStrings map[SDType]string = map[SDType]string{SDNone: "", SDNginx: "nginx"}

func (a *SDType) Set(value string) error {
	switch value {
	case "nginx":
		*a = SDNginx
	case "", "0":
		*a = SDNone
	default:
		return fmt.Errorf("invalid sd type %q", value)
	}

	return nil
}

func (a *SDType) UnmarshalText(data []byte) error {
	return a.Set(string(data))
}

func (a *SDType) MarshalText() ([]byte, error) {
	return []byte(a.String()), nil
}

func (a *SDType) UnmarshalJSON(data []byte) error {
	return a.Set(string(data))
}

func (a *SDType) MarshalJSON() ([]byte, error) {
	return []byte(a.String()), nil
}

func (a *SDType) String() string {
	return sdTypeStrings[*a]
}

func (a *SDType) Type() string {
	return "service_discovery_type"
}

// Cache config
type CacheConfig struct {
	Type                string        `toml:"type"              json:"type"              comment:"cache type"`
	Size                int           `toml:"size-mb"           json:"size-mb"           comment:"cache size"`
	MemcachedServers    []string      `toml:"memcached-servers" json:"memcached-servers" comment:"memcached servers"`
	DefaultTimeoutSec   int32         `toml:"default-timeout"   json:"default-timeout"   comment:"default cache ttl"`
	DefaultTimeoutStr   string        `toml:"-"                 json:"-"`
	ShortTimeoutSec     int32         `toml:"short-timeout"     json:"short-timeout"     comment:"short-time cache ttl"`
	ShortTimeoutStr     string        `toml:"-"                 json:"-"`
	FindTimeoutSec      int32         `toml:"find-timeout"      json:"find-timeout"      comment:"finder/tags autocompleter cache ttl"`
	ShortDuration       time.Duration `toml:"short-duration"    json:"short-duration"    comment:"maximum diration, used with short_timeout"`
	ShortUntilOffsetSec int64         `toml:"short-offset"      json:"short-offset"      comment:"offset beetween now and until for select short cache timeout"`
}

// Common config
type Common struct {
	Listen                 string           `toml:"listen"                     json:"listen"                     comment:"general listener"`
	PprofListen            string           `toml:"pprof-listen"               json:"pprof-listen"               comment:"listener to serve /debug/pprof requests. '-pprof' argument overrides it"`
	MaxCPU                 int              `toml:"max-cpu"                    json:"max-cpu"`
	MaxMetricsInFindAnswer int              `toml:"max-metrics-in-find-answer" json:"max-metrics-in-find-answer" comment:"limit number of results from find query, 0=unlimited"`
	MaxMetricsPerTarget    int              `toml:"max-metrics-per-target"     json:"max-metrics-per-target"     comment:"limit numbers of queried metrics per target in /render requests, 0 or negative = unlimited"`
	AppendEmptySeries      bool             `toml:"append-empty-series"        json:"append-empty-series"        comment:"if true, always return points for all metrics, replacing empty results with list of NaN"`
	TargetBlacklist        []string         `toml:"target-blacklist"           json:"target-blacklist"           comment:"daemon returns empty response if query matches any of regular expressions"                  commented:"true"`
	Blacklist              []*regexp.Regexp `toml:"-"                          json:"-"` // compiled TargetBlacklist
	MemoryReturnInterval   time.Duration    `toml:"memory-return-interval"     json:"memory-return-interval"     comment:"daemon will return the freed memory to the OS when it>0"`
	HeadersToLog           []string         `toml:"headers-to-log"             json:"headers-to-log"             comment:"additional request headers to log"`

	BaseWeight       int           `toml:"base_weight"            json:"base_weight"            comment:"service discovery base weight (on idle)"`
	DegragedMultiply float64       `toml:"degraged-multiply"            json:"degraged-multiply"            comment:"service discovery degraded load avg multiplier (if normalized load avg > degraged_load_avg) (default 4.0)"`
	DegragedLoad     float64       `toml:"degraged-load-avg"            json:"degraged-load-avg"            comment:"service discovery normilized load avg degraded point (default 1.0)"`
	SDType           SDType        `toml:"service-discovery-type" json:"service-discovery-type" comment:"service discovery type"`
	SD               string        `toml:"service-discovery"      json:"service-discovery"      comment:"service discovery address (consul)"`
	SDNamespace      string        `toml:"service-discovery-ns"   json:"service-discovery-ns"   comment:"service discovery namespace (graphite by default)"`
	SDDc             []string      `toml:"service-discovery-ds"   json:"service-discovery-ds"   comment:"service discovery datacenters (first - is primary, in other register as backup)"`
	SDExpire         time.Duration `toml:"service-discovery-expire"   json:"service-discovery-expire"   comment:"service discovery expire duration for cleanup (minimum is 24h, if enabled)"`

	FindCacheConfig CacheConfig `toml:"find-cache"      json:"find-cache"             comment:"find/tags cache config"`

	FindCache cache.BytesCache `toml:"-" json:"-"`
}

// FeatureFlags contains feature flags that significantly change how gch responds to some requests
type FeatureFlags struct {
	UseCarbonBehavior    bool `toml:"use-carbon-behaviour" json:"use-carbon-behaviour" comment:"if true, prefers carbon's behaviour on how tags are treated"`
	DontMatchMissingTags bool `toml:"dont-match-missing-tags" json:"dont-match-missing-tags" comment:"if true, seriesByTag terms containing '!=' or '!=~' operators will not match metrics that don't have the tag at all"`
	LogQueryProgress     bool `toml:"log-query-progress" json:"log-query-progress" comment:"if true, gch will log affected rows count by clickhouse query"`
}

// IndexReverseRule contains rules to use direct or reversed request to index table
type IndexReverseRule struct {
	Suffix   string         `toml:"suffix,omitempty" json:"suffix"  comment:"rule is used when the target suffix is matched"`
	Prefix   string         `toml:"prefix,omitempty" json:"prefix"  comment:"rule is used when the target prefix is matched"`
	RegexStr string         `toml:"regex,omitempty"  json:"regex"   comment:"rule is used when the target regex is matched"`
	Regex    *regexp.Regexp `toml:"-"                json:"-"`
	Reverse  string         `toml:"reverse"          json:"reverse" comment:"same as index-reverse"`
}

type Costs struct {
	Cost       *int           `toml:"cost"        json:"cost"        comment:"default cost (for wildcarded equalence or matched with regex, or if no value cost set)"`
	ValuesCost map[string]int `toml:"values-cost" json:"values-cost" comment:"cost with some value (for equalence without wildcards) (additional tuning, usually not needed)"`
}

// IndexReverses is a slise of ptrs to IndexReverseRule
type IndexReverses []*IndexReverseRule

const (
	IndexAuto     = iota
	IndexDirect   = iota
	IndexReversed = iota
)

// IndexReverse maps setting name to value
var IndexReverse = map[string]uint8{
	"direct":   IndexDirect,
	"auto":     IndexAuto,
	"reversed": IndexReversed,
}

// IndexReverseNames contains valid names for index-reverse setting
var IndexReverseNames = []string{"auto", "direct", "reversed"}

type UserLimits struct {
	MaxQueries        int `toml:"max-queries"      json:"max-queries"  comment:"Max queries to fetch data"`
	ConcurrentQueries int `toml:"concurrent-queries" json:"concurrent-queries" comment:"Concurrent queries to fetch data"`
	AdaptiveQueries   int `toml:"adaptive-queries" json:"adaptive-queries" comment:"Adaptive queries (based on load average) for increase/decrease concurrent queries"`

	Limiter limiter.ServerLimiter `toml:"-" json:"-"`
}

type QueryParam struct {
	Duration    time.Duration `toml:"duration"     json:"duration"     comment:"minimal duration (beetween from/until) for select query params"`
	URL         string        `toml:"url"          json:"url"          comment:"url for queries with durations greater or equal than"`
	DataTimeout time.Duration `toml:"data-timeout" json:"data-timeout" comment:"total timeout to fetch data"`

	MaxQueries        int `toml:"max-queries" json:"max-queries" comment:"Max queries to fetch data"`
	ConcurrentQueries int `toml:"concurrent-queries" json:"concurrent-queries" comment:"Concurrent queries to fetch data"`
	AdaptiveQueries   int `toml:"adaptive-queries" json:"adaptive-queries" comment:"Adaptive queries (based on load average) for increase/decrease concurrent queries"`

	Limiter limiter.ServerLimiter `toml:"-" json:"-"`
}

func binarySearchQueryParamLe(a []QueryParam, duration time.Duration, start, end int) int {
	length := end - start
	if length <= 0 {
		return -1 // not found
	} else if length == 1 {
		if a[start].Duration > duration {
			return -1
		}

		return start
	}

	var result int

	mid := start + length/2
	if a[mid].Duration > duration {
		result = binarySearchQueryParamLe(a, duration, start, mid)
	} else {
		if result = binarySearchQueryParamLe(a, duration, mid+1, end); result == -1 {
			result = mid
		}
	}

	return result
}

// ClickHouse config
type ClickHouse struct {
	URL         string        `toml:"url"                      json:"url"                      comment:"default url, see https://clickhouse.tech/docs/en/interfaces/http. Can be overwritten with query-params"`
	DataTimeout time.Duration `toml:"data-timeout"             json:"data-timeout"             comment:"default total timeout to fetch data, can be overwritten with query-params"`
	QueryParams []QueryParam  `toml:"query-params"             json:"query-params"             comment:"customized query params (url, data timeout, limiters) for durations greater or equal"`

	ProgressSendingInterval time.Duration `toml:"progress-sending-interval" json:"progress-sending-interval" comment:"time interval for ch query progress sending, it's equal to http_headers_progress_interval_ms header"`

	RenderMaxQueries        int `toml:"render-max-queries" json:"render-max-queries" comment:"Max queries to render queiries"`
	RenderConcurrentQueries int `toml:"render-concurrent-queries" json:"render-concurrent-queries" comment:"Concurrent queries to render queiries"`
	RenderAdaptiveQueries   int `toml:"render-adaptive-queries" json:"render-adaptive-queries" comment:"Render adaptive queries (based on load average) for increase/decrease concurrent queries"`

	FindMaxQueries        int                   `toml:"find-max-queries" json:"find-max-queries" comment:"Max queries for find queries"`
	FindConcurrentQueries int                   `toml:"find-concurrent-queries" json:"find-concurrent-queries" comment:"Find concurrent queries for find queries"`
	FindAdaptiveQueries   int                   `toml:"find-adaptive-queries" json:"find-adaptive-queries" comment:"Find adaptive queries (based on load average) for increase/decrease concurrent queries"`
	FindLimiter           limiter.ServerLimiter `toml:"-"                        json:"-"`

	TagsMaxQueries        int                   `toml:"tags-max-queries" json:"tags-max-queries" comment:"Max queries for tags queries"`
	TagsConcurrentQueries int                   `toml:"tags-concurrent-queries" json:"tags-concurrent-queries" comment:"Concurrent queries for tags queries"`
	TagsAdaptiveQueries   int                   `toml:"tags-adaptive-queries" json:"tags-adaptive-queries" comment:"Tags adaptive queries (based on load average) for increase/decrease concurrent queries"`
	TagsLimiter           limiter.ServerLimiter `toml:"-"                        json:"-"`

	WildcardMinDistance   int  `toml:"wildcard-min-distance" json:"wildcard-min-distance" comment:"If a wildcard appears both at the start and the end of a plain query at a distance (in terms of nodes) less than wildcard-min-distance, then it will be discarded. This parameter can be used to discard expensive queries."`
	TrySplitQuery         bool `toml:"try-split-query" json:"try-split-query" comment:"Plain queries like '{first,second}.custom.metric.*' are also a subject to wildcard-min-distance restriction. But can be split into 2 queries: 'first.custom.metric.*', 'second.custom.metric.*'. Note that: only one list will be split; if there are wildcard in query before (after) list then reverse (direct) notation will be preferred; if there are wildcards before and after list, then query will not be split"`
	MaxNodeToSplitIndex   int  `toml:"max-node-to-split-index" json:"max-node-to-split-index" comment:"Used only if try-split-query is true. Query that contains list will be split if its (list) node index is less or equal to max-node-to-split-index. By default is 0. It is recommended to have this value set to 2 or 3 and increase it very carefully, because 3 or 4 plain nodes without wildcards have good selectivity"`
	TagsMinInQuery        int  `toml:"tags-min-in-query" json:"tags-min-in-query" comment:"Minimum tags in seriesByTag query"`
	TagsMinInAutocomplete int  `toml:"tags-min-in-autocomplete" json:"tags-min-in-autocomplete" comment:"Minimum tags in autocomplete query"`

	UserLimits           map[string]UserLimits `toml:"user-limits"              json:"user-limits"              comment:"customized query limiter for some users"                                                                                        commented:"true"`
	DateFormat           string                `toml:"date-format"              json:"date-format"              comment:"Date format (default, utc, both)"`
	IndexTable           string                `toml:"index-table"              json:"index-table"              comment:"see doc/index-table.md"`
	IndexUseDaily        bool                  `toml:"index-use-daily"          json:"index-use-daily"`
	IndexReverse         string                `toml:"index-reverse"            json:"index-reverse"            comment:"see doc/config.md"`
	IndexReverses        IndexReverses         `toml:"index-reverses"           json:"index-reverses"           comment:"see doc/config.md"                                                                                                              commented:"true"`
	IndexTimeout         time.Duration         `toml:"index-timeout"            json:"index-timeout"            comment:"total timeout to fetch series list from index"`
	TaggedTable          string                `toml:"tagged-table"             json:"tagged-table"             comment:"'tagged' table from carbon-clickhouse, required for seriesByTag"`
	TagsCountTable       string                `toml:"tags-count-table"         json:"tags-count-table"         comment:"Table that contains the total amounts of each tag-value pair. It is used to avoid usage of high cardinality tag-value pairs when querying TaggedTable. If left empty, basic sorting will be used. See more detailed description in doc/config.md"`
	TaggedAutocompleDays int                   `toml:"tagged-autocomplete-days" json:"tagged-autocomplete-days" comment:"or how long the daemon will query tags during autocomplete"`
	TaggedUseDaily       bool                  `toml:"tagged-use-daily"         json:"tagged-use-daily"         comment:"whether to use date filter when searching for the metrics in the tagged-table"`
	TaggedCosts          map[string]*Costs     `toml:"tagged-costs"             json:"tagged-costs"             comment:"costs for tags (for tune which tag will be used as primary), by default is 0, increase for costly (with poor selectivity) tags" commented:"true"`
	TreeTable            string                `toml:"tree-table"               json:"tree-table"               comment:"old index table, DEPRECATED, see description in doc/config.md"                                                                  commented:"true"`
	ReverseTreeTable     string                `toml:"reverse-tree-table"       json:"reverse-tree-table"                                                                                                                                                commented:"true"`
	DateTreeTable        string                `toml:"date-tree-table"          json:"date-tree-table"                                                                                                                                                   commented:"true"`
	DateTreeTableVersion int                   `toml:"date-tree-table-version"  json:"date-tree-table-version"                                                                                                                                           commented:"true"`
	TreeTimeout          time.Duration         `toml:"tree-timeout"             json:"tree-timeout"                                                                                                                                                      commented:"true"`
	TagTable             string                `toml:"tag-table"                json:"tag-table"                comment:"is not recommended to use, https://github.com/lomik/graphite-clickhouse/wiki/TagsRU"                                            commented:"true"`
	ExtraPrefix          string                `toml:"extra-prefix"             json:"extra-prefix"             comment:"add extra prefix (directory in graphite) for all metrics, w/o trailing dot"`
	ConnectTimeout       time.Duration         `toml:"connect-timeout"          json:"connect-timeout"          comment:"TCP connection timeout"`
	// TODO: remove in v0.14
	DataTableLegacy string `toml:"data-table"               json:"data-table"               comment:"will be removed in 0.14"                                                                                                        commented:"true"`
	// TODO: remove in v0.14
	RollupConfLegacy string `toml:"rollup-conf"              json:"-"                                                                                                                                                                 commented:"true"`
	MaxDataPoints    int    `toml:"max-data-points"          json:"max-data-points"          comment:"max points per metric when internal-aggregation=true"`
	// InternalAggregation controls if ClickHouse itself or graphite-clickhouse aggregates points to proper retention
	InternalAggregation bool `toml:"internal-aggregation"     json:"internal-aggregation"     comment:"ClickHouse-side aggregation, see doc/aggregation.md"`

	TLSParams config.TLS  `toml:"tls"                      json:"tls"                      comment:"mTLS HTTPS configuration for connecting to clickhouse server"                                                                         commented:"true"`
	TLSConfig *tls.Config `toml:"-"                        json:"-"`
}

func clickhouseURLValidate(chURL string) (*url.URL, error) {
	u, err := url.Parse(chURL)
	if err != nil {
		return nil, fmt.Errorf("error %q in url %q", err.Error(), chURL)
	} else if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("scheme not supported in url %q", chURL)
	} else if strings.Contains(u.RawQuery, " ") {
		return nil, fmt.Errorf("space not allowed in url %q", chURL)
	}

	return u, nil
}

// Tags config
type Tags struct {
	Rules             string                     `toml:"rules"               json:"rules"`
	Date              string                     `toml:"date"                json:"date"`
	ExtraWhere        string                     `toml:"extra-where"         json:"extra-where"`
	InputFile         string                     `toml:"input-file"          json:"input-file"`
	OutputFile        string                     `toml:"output-file"         json:"output-file"`
	Threads           int                        `toml:"threads"             json:"threads"              comment:"number of threads for uploading tags to clickhouse (1 by default)"`
	Compression       clickhouse.ContentEncoding `toml:"compression"         json:"compression"          comment:"compression method for tags before sending them to clickhouse (i.e. content encoding): gzip (default), none, zstd"`
	Version           uint32                     `toml:"version"             json:"version"              comment:"fixed tags version for testing purposes (by default the current timestamp is used for each upload)"`
	SelectChunksCount int                        `toml:"select-chunks-count" json:"select-chunks-count"  comment:"number of chunks for selecting metrics from clickhouse (10 by default)"`
}

// Carbonlink configuration
type Carbonlink struct {
	Server         string        `toml:"server"              json:"server"`
	Threads        int           `toml:"threads-per-request" json:"threads-per-request"`
	Retries        int           `toml:"-"                   json:"-"`
	ConnectTimeout time.Duration `toml:"connect-timeout"     json:"connect-timeout"`
	QueryTimeout   time.Duration `toml:"query-timeout"       json:"query-timeout"`
	TotalTimeout   time.Duration `toml:"total-timeout"       json:"total-timeout"       comment:"timeout for querying and parsing response"`
}

// Prometheus configuration
type Prometheus struct {
	Listen                     string        `toml:"listen"                        json:"listen"                        comment:"listen addr for prometheus ui and api"`
	ExternalURLRaw             string        `toml:"external-url"                  json:"external-url"                  comment:"allows to set URL for redirect manually"`
	ExternalURL                *url.URL      `toml:"-"                             json:"-"`
	PageTitle                  string        `toml:"page-title"                    json:"page-title"`
	LookbackDelta              time.Duration `toml:"lookback-delta"                json:"lookback-delta"`
	RemoteReadConcurrencyLimit int           `toml:"remote-read-concurrency-limit" json:"remote-read-concurrency-limit" comment:"concurrently handled remote read requests"`
}

const (
	// ContextGraphite for data tables
	ContextGraphite = "graphite"
	// ContextPrometheus for data tables
	ContextPrometheus = "prometheus"
)

var knownDataTableContext = map[string]bool{
	ContextGraphite:   true,
	ContextPrometheus: true,
}

// DataTable configs
type DataTable struct {
	Table                  string                `toml:"table"                    json:"table"                    comment:"data table from carbon-clickhouse"`
	Reverse                bool                  `toml:"reverse"                  json:"reverse"                  comment:"if it stores direct or reversed metrics"`
	MaxAge                 time.Duration         `toml:"max-age"                  json:"max-age"                  comment:"maximum age stored in the table"`
	MinAge                 time.Duration         `toml:"min-age"                  json:"min-age"                  comment:"minimum age stored in the table"`
	MaxInterval            time.Duration         `toml:"max-interval"             json:"max-interval"             comment:"maximum until-from interval allowed for the table"`
	MinInterval            time.Duration         `toml:"min-interval"             json:"min-interval"             comment:"minimum until-from interval allowed for the table"`
	TargetMatchAny         string                `toml:"target-match-any"         json:"target-match-any"         comment:"table allowed only if any metrics in target matches regexp"`
	TargetMatchAll         string                `toml:"target-match-all"         json:"target-match-all"         comment:"table allowed only if all metrics in target matches regexp"`
	TargetMatchAnyRegexp   *regexp.Regexp        `toml:"-"                        json:"-"`
	TargetMatchAllRegexp   *regexp.Regexp        `toml:"-"                        json:"-"`
	RollupConf             string                `toml:"rollup-conf"              json:"-"                        comment:"custom rollup.xml file for table, 'auto' and 'none' are allowed as well"`
	RollupAutoTable        string                `toml:"rollup-auto-table"        json:"rollup-auto-table"        comment:"custom table for 'rollup-conf=auto', useful for Distributed or MatView"`
	RollupAutoInterval     *time.Duration        `toml:"rollup-auto-interval"     json:"rollup-auto-interval"     comment:"rollup update interval for 'rollup-conf=auto'"`
	RollupDefaultPrecision uint32                `toml:"rollup-default-precision" json:"rollup-default-precision" comment:"is used when none of rules match"`
	RollupDefaultFunction  string                `toml:"rollup-default-function"  json:"rollup-default-function"  comment:"is used when none of rules match"`
	RollupUseReverted      bool                  `toml:"rollup-use-reverted"      json:"rollup-use-reverted"      comment:"should be set to true if you don't have reverted regexps in rollup-conf for reversed tables"`
	Context                []string              `toml:"context"                  json:"context"                  comment:"valid values are 'graphite' of 'prometheus'"`
	ContextMap             map[string]bool       `toml:"-"                        json:"-"`
	Rollup                 *rollup.Rollup        `toml:"-"                        json:"rollup-conf"`
	QueryMetrics           *metrics.QueryMetrics `toml:"-"                        json:"-"`
}

// Debug config
type Debug struct {
	Directory     string      `toml:"directory"          json:"directory"          comment:"the directory for additional debug output"`
	DirectoryPerm os.FileMode `toml:"directory-perm"     json:"directory-perm"     comment:"permissions for directory, octal value is set as 0o755"`
	// If ExternalDataPerm > 0 and X-Gch-Debug-Ext-Data HTTP header is set, the external data used in the query
	// will be saved in the DebugDir directory
	ExternalDataPerm os.FileMode `toml:"external-data-perm" json:"external-data-perm" comment:"permissions for directory, octal value is set as 0o640"`
}

// Config is the daemon configuration
type Config struct {
	Common       Common             `toml:"common"        json:"common"`
	FeatureFlags FeatureFlags       `toml:"feature-flags" json:"feature-flags"`
	Metrics      metrics.Config     `toml:"metrics"       json:"metrics"`
	ClickHouse   ClickHouse         `toml:"clickhouse"    json:"clickhouse"`
	DataTable    []DataTable        `toml:"data-table"    json:"data-table" comment:"data tables, see doc/config.md for additional info"`
	Tags         Tags               `toml:"tags"          json:"tags"       comment:"is not recommended to use, https://github.com/lomik/graphite-clickhouse/wiki/TagsRU" commented:"true"`
	Carbonlink   Carbonlink         `toml:"carbonlink"    json:"carbonlink"`
	Prometheus   Prometheus         `toml:"prometheus"    json:"prometheus"`
	Debug        Debug              `toml:"debug"         json:"debug"      comment:"see doc/debugging.md"`
	Logging      []zapwriter.Config `toml:"logging"       json:"logging"`
}

// New returns *Config with default values
func New() *Config {
	cfg := &Config{
		Common: Common{
			Listen:      ":9090",
			PprofListen: "",
			// MetricPrefix: "carbon.graphite-clickhouse.{host}",
			// MetricInterval: time.Minute,
			// MetricEndpoint: MetricEndpointLocal,
			MaxCPU:                 1,
			MaxMetricsInFindAnswer: 0,
			MaxMetricsPerTarget:    15000, // This is arbitrary value to protect CH from overload
			MemoryReturnInterval:   0,
			FindCacheConfig: CacheConfig{
				Type:              "null",
				DefaultTimeoutSec: 0,
				ShortTimeoutSec:   0,
				FindTimeoutSec:    0,
			},
			DegragedMultiply: 4.0,
			DegragedLoad:     1.0,
		},
		ClickHouse: ClickHouse{
			URL:                     "http://localhost:8123?cancel_http_readonly_queries_on_client_close=1",
			DataTimeout:             time.Minute,
			ProgressSendingInterval: 10 * time.Second,
			IndexTable:              "graphite_index",
			IndexUseDaily:           true,
			TaggedUseDaily:          true,
			IndexReverse:            "auto",
			IndexReverses:           IndexReverses{},
			IndexTimeout:            time.Minute,
			TaggedTable:             "graphite_tagged",
			TaggedAutocompleDays:    7,
			ExtraPrefix:             "",
			ConnectTimeout:          time.Second,
			DataTableLegacy:         "",
			RollupConfLegacy:        "auto",
			MaxDataPoints:           1048576,
			InternalAggregation:     true,
			FindLimiter:             limiter.NoopLimiter{},
			TagsLimiter:             limiter.NoopLimiter{},
		},
		Tags: Tags{
			Threads:     1,
			Compression: "gzip",
		},
		Carbonlink: Carbonlink{
			Threads:        10,
			Retries:        2,
			ConnectTimeout: 50 * time.Millisecond,
			QueryTimeout:   50 * time.Millisecond,
			TotalTimeout:   500 * time.Millisecond,
		},
		Prometheus: Prometheus{
			ExternalURLRaw:             "",
			PageTitle:                  "Prometheus Time Series Collection and Processing Server",
			Listen:                     ":9092",
			LookbackDelta:              5 * time.Minute,
			RemoteReadConcurrencyLimit: 10,
		},
		Debug: Debug{
			Directory:        "",
			DirectoryPerm:    0755,
			ExternalDataPerm: 0,
		},
		Logging: nil,
	}

	return cfg
}

// Compile checks if IndexReverseRule are valid in the IndexReverses and compiles regexps if set
func (ir IndexReverses) Compile() error {
	var err error

	for i, n := range ir {
		if len(n.RegexStr) > 0 {
			if n.Regex, err = regexp.Compile(n.RegexStr); err != nil {
				return err
			}
		} else if len(n.Prefix) == 0 && len(n.Suffix) == 0 {
			return fmt.Errorf("empthy index-use-reverses[%d] rule", i)
		}

		if _, ok := IndexReverse[n.Reverse]; !ok {
			return fmt.Errorf("%s is not valid value for index-reverses.reverse", n.Reverse)
		}
	}

	return nil
}

func newLoggingConfig() zapwriter.Config {
	cfg := zapwriter.NewConfig()
	cfg.File = "/var/log/graphite-clickhouse/graphite-clickhouse.log"

	return cfg
}

func DefaultConfig() (*Config, error) {
	cfg := New()

	if cfg.Logging == nil {
		cfg.Logging = make([]zapwriter.Config, 0)
	}

	if len(cfg.Logging) == 0 {
		cfg.Logging = append(cfg.Logging, newLoggingConfig())
	}

	if len(cfg.DataTable) == 0 {
		interval := time.Minute
		cfg.DataTable = []DataTable{
			{
				Table:              "graphite_data",
				RollupConf:         "auto",
				RollupAutoInterval: &interval,
			},
		}
	}

	if len(cfg.ClickHouse.IndexReverses) == 0 {
		cfg.ClickHouse.IndexReverses = IndexReverses{
			&IndexReverseRule{Suffix: "suffix", Reverse: "auto"},
			&IndexReverseRule{Prefix: "prefix", Reverse: "direct"},
			&IndexReverseRule{RegexStr: "regex", Reverse: "reversed"},
		}

		err := cfg.ClickHouse.IndexReverses.Compile()
		if err != nil {
			return nil, err
		}
	}

	return cfg, nil
}

// PrintDefaultConfig prints the default config with some additions to be useful
func PrintDefaultConfig() error {
	buf := new(bytes.Buffer)

	cfg, err := DefaultConfig()
	if err != nil {
		return err
	}

	encoder := toml.NewEncoder(buf).Indentation(" ").Order(toml.OrderPreserve).CompactComments(true)

	if err := encoder.Encode(cfg); err != nil {
		return err
	}

	out := strings.Replace(buf.String(), "\n", "", 1)

	fmt.Print(out)

	return nil
}

// ReadConfig reads the content of the file with given name and process it to the *Config
func ReadConfig(filename string, exactConfig bool) (*Config, []zap.Field, error) {
	var err error

	var body []byte
	if filename != "" {
		body, err = os.ReadFile(filename)
		if err != nil {
			return nil, nil, err
		}
	}

	return Unmarshal(body, exactConfig)
}

// Unmarshal process the body to *Config
func Unmarshal(body []byte, exactConfig bool) (cfg *Config, warns []zap.Field, err error) {
	deprecations := make(map[string]error)

	cfg = New()

	if len(body) != 0 {
		// TODO: remove in v0.14
		if bytes.Index(body, []byte("\n[logging]\n")) != -1 || bytes.Index(body, []byte("[logging]")) == 0 {
			deprecations["logging"] = fmt.Errorf("single [logging] value became multivalue [[logging]]; please, adjust your config")

			body = bytes.ReplaceAll(body, []byte("\n[logging]\n"), []byte("\n[[logging]]\n"))
			if bytes.Index(body, []byte("[logging]")) == 0 {
				body = bytes.Replace(body, []byte("[logging]"), []byte("[[logging]]"), 1)
			}
		}

		decoder := toml.NewDecoder(bytes.NewReader(body))
		decoder.Strict(exactConfig)

		err := decoder.Decode(cfg)
		if err != nil {
			return nil, nil, err
		}
	}

	if cfg.Logging == nil {
		cfg.Logging = make([]zapwriter.Config, 0)
	}

	if cfg.ClickHouse.RenderConcurrentQueries > cfg.ClickHouse.RenderMaxQueries && cfg.ClickHouse.RenderMaxQueries > 0 {
		cfg.ClickHouse.RenderConcurrentQueries = 0
	}

	chURL, err := clickhouseURLValidate(cfg.ClickHouse.URL)
	if err != nil {
		return nil, nil, err
	}

	if !reflect.DeepEqual(cfg.ClickHouse.TLSParams, config.TLS{}) {
		tlsConfig, warnings, err := config.ParseClientTLSConfig(&cfg.ClickHouse.TLSParams)
		if err != nil {
			return nil, nil, err
		}

		if chURL.Scheme == "https" {
			cfg.ClickHouse.TLSConfig = tlsConfig
		} else {
			warnings = append(warnings, "TLS configurations is ignored because scheme is not HTTPS")
		}

		warns = append(warns, zap.Strings("tls-config", warnings))
	}

	for i := range cfg.ClickHouse.QueryParams {
		if cfg.ClickHouse.QueryParams[i].ConcurrentQueries > cfg.ClickHouse.QueryParams[i].MaxQueries && cfg.ClickHouse.QueryParams[i].MaxQueries > 0 {
			cfg.ClickHouse.QueryParams[i].ConcurrentQueries = 0
		}

		if cfg.ClickHouse.QueryParams[i].Duration == 0 {
			return nil, nil, fmt.Errorf("query duration param not set for: %+v", cfg.ClickHouse.QueryParams[i])
		}

		if cfg.ClickHouse.QueryParams[i].DataTimeout == 0 {
			cfg.ClickHouse.QueryParams[i].DataTimeout = cfg.ClickHouse.DataTimeout
		}

		if cfg.ClickHouse.QueryParams[i].URL == "" {
			// reuse default url
			cfg.ClickHouse.QueryParams[i].URL = cfg.ClickHouse.URL
		}

		if _, err = clickhouseURLValidate(cfg.ClickHouse.QueryParams[i].URL); err != nil {
			return nil, nil, err
		}
	}

	cfg.ClickHouse.QueryParams = append(
		[]QueryParam{{
			URL: cfg.ClickHouse.URL, DataTimeout: cfg.ClickHouse.DataTimeout,
			MaxQueries: cfg.ClickHouse.RenderMaxQueries, ConcurrentQueries: cfg.ClickHouse.RenderConcurrentQueries,
			AdaptiveQueries: cfg.ClickHouse.RenderAdaptiveQueries,
		}},
		cfg.ClickHouse.QueryParams...,
	)

	sort.SliceStable(cfg.ClickHouse.QueryParams, func(i, j int) bool {
		return cfg.ClickHouse.QueryParams[i].Duration < cfg.ClickHouse.QueryParams[j].Duration
	})

	if len(cfg.Logging) == 0 {
		cfg.Logging = append(cfg.Logging, newLoggingConfig())
	}

	if err = zapwriter.CheckConfig(cfg.Logging, nil); err != nil {
		return nil, nil, err
	}

	// Check if debug directory exists or could be created
	if cfg.Debug.Directory != "" {
		info, err := os.Stat(cfg.Debug.Directory)
		if os.IsNotExist(err) {
			err := os.MkdirAll(cfg.Debug.Directory, os.ModeDir|cfg.Debug.DirectoryPerm)
			if err != nil {
				return nil, nil, err
			}
		} else if !info.IsDir() {
			return nil, nil, fmt.Errorf("the file for external data debug dumps exists and is not a directory: %v", cfg.Debug.Directory)
		}
	}

	if _, ok := IndexReverse[cfg.ClickHouse.IndexReverse]; !ok {
		return nil, nil, fmt.Errorf("%s is not valid value for index-reverse", cfg.ClickHouse.IndexReverse)
	}

	err = cfg.ClickHouse.IndexReverses.Compile()
	if err != nil {
		return nil, nil, err
	}

	if cfg.Common.FindCache, err = CreateCache("index", &cfg.Common.FindCacheConfig); err == nil {
		if cfg.Common.FindCacheConfig.Type != "null" {
			warns = append(warns, zap.Any("enable find cache", zap.String("type", cfg.Common.FindCacheConfig.Type)))
		}
	} else {
		return nil, nil, err
	}

	l := len(cfg.Common.TargetBlacklist)
	if l > 0 {
		cfg.Common.Blacklist = make([]*regexp.Regexp, l)
		for i := 0; i < l; i++ {
			r, err := regexp.Compile(cfg.Common.TargetBlacklist[i])
			if err != nil {
				return nil, nil, err
			}

			cfg.Common.Blacklist[i] = r
		}
	}

	err = cfg.ProcessDataTables()
	if err != nil {
		return nil, nil, err
	}

	// compute prometheus external url
	rawURL := cfg.Prometheus.ExternalURLRaw
	if rawURL == "" {
		hostname, err := os.Hostname()
		if err != nil {
			return nil, nil, err
		}

		_, port, err := net.SplitHostPort(cfg.Common.Listen)
		if err != nil {
			return nil, nil, err
		}

		rawURL = fmt.Sprintf("http://%s:%s/", hostname, port)
	}

	cfg.Prometheus.ExternalURL, err = url.Parse(rawURL)
	if err != nil {
		return nil, nil, err
	}

	cfg.Prometheus.ExternalURL.Path = strings.TrimRight(cfg.Prometheus.ExternalURL.Path, "/")

	checkDeprecations(cfg, deprecations)

	if len(deprecations) != 0 {
		deprecationList := make([]error, len(deprecations))
		for name, message := range deprecations {
			deprecationList = append(deprecationList, errors.Wrap(message, name))
		}

		warns = append(warns, zap.Errors("config deprecations", deprecationList))
	}

	switch strings.ToLower(cfg.ClickHouse.DateFormat) {
	case "utc":
		date.SetUTC()
	case "both":
		date.SetBoth()
	default:
		if cfg.ClickHouse.DateFormat != "" && cfg.ClickHouse.DateFormat != "default" {
			return nil, nil, fmt.Errorf("unsupported date-format: %s", cfg.ClickHouse.DateFormat)
		}
	}

	if cfg.ClickHouse.FindConcurrentQueries > cfg.ClickHouse.FindMaxQueries && cfg.ClickHouse.FindMaxQueries > 0 {
		cfg.ClickHouse.FindConcurrentQueries = 0
	}

	if cfg.ClickHouse.TagsConcurrentQueries > cfg.ClickHouse.TagsMaxQueries && cfg.ClickHouse.TagsMaxQueries > 0 {
		cfg.ClickHouse.TagsConcurrentQueries = 0
	}

	metricsEnabled := cfg.setupGraphiteMetrics()

	cfg.ClickHouse.FindLimiter = limiter.NewALimiter(
		cfg.ClickHouse.FindMaxQueries, cfg.ClickHouse.FindConcurrentQueries, cfg.ClickHouse.FindAdaptiveQueries,
		metricsEnabled, "find", "all",
	)

	cfg.ClickHouse.TagsLimiter = limiter.NewALimiter(
		cfg.ClickHouse.TagsMaxQueries, cfg.ClickHouse.TagsConcurrentQueries, cfg.ClickHouse.TagsAdaptiveQueries,
		metricsEnabled, "tags", "all",
	)

	for i := range cfg.ClickHouse.QueryParams {
		cfg.ClickHouse.QueryParams[i].Limiter = limiter.NewALimiter(
			cfg.ClickHouse.QueryParams[i].MaxQueries, cfg.ClickHouse.QueryParams[i].ConcurrentQueries,
			cfg.ClickHouse.QueryParams[i].AdaptiveQueries,
			metricsEnabled, "render", duration.String(cfg.ClickHouse.QueryParams[i].Duration),
		)
	}

	for u, q := range cfg.ClickHouse.UserLimits {
		q.Limiter = limiter.NewALimiter(
			q.MaxQueries, q.ConcurrentQueries, q.AdaptiveQueries, metricsEnabled, u, "all",
		)
		cfg.ClickHouse.UserLimits[u] = q
	}

	return cfg, warns, nil
}

// NeedLoadAvgColect check if load avg collect is neeeded
func (c *Config) NeedLoadAvgColect() bool {
	if c.Common.SD != "" {
		if c.Common.DegragedMultiply <= 0 {
			c.Common.DegragedMultiply = 4.0
		}

		if c.Common.DegragedLoad <= 0 {
			c.Common.DegragedLoad = 1.0
		}

		if c.Common.BaseWeight <= 0 {
			c.Common.BaseWeight = 100
		}

		if c.Common.SDNamespace == "" {
			c.Common.SDNamespace = "graphite"
		}

		if c.Common.SDExpire < 24*time.Hour {
			c.Common.SDExpire = 24 * time.Hour
		}

		return true
	}

	if c.ClickHouse.RenderAdaptiveQueries > 0 {
		return true
	}

	if c.ClickHouse.FindAdaptiveQueries > 0 {
		return true
	}

	if c.ClickHouse.TagsAdaptiveQueries > 0 {
		return true
	}

	for _, u := range c.ClickHouse.UserLimits {
		if u.AdaptiveQueries > 0 {
			return true
		}
	}

	return false
}

// ProcessDataTables checks if legacy `data`-table config is used, compiles regexps for `target-match-any` and `target-match-all`
// parameters, sets the rollup configuration and proper context.
func (c *Config) ProcessDataTables() (err error) {
	if c.ClickHouse.DataTableLegacy != "" {
		c.DataTable = append(c.DataTable, DataTable{
			Table:      c.ClickHouse.DataTableLegacy,
			RollupConf: c.ClickHouse.RollupConfLegacy,
		})
	}

	for i := 0; i < len(c.DataTable); i++ {
		if c.DataTable[i].TargetMatchAny != "" {
			r, err := regexp.Compile(c.DataTable[i].TargetMatchAny)
			if err != nil {
				return err
			}

			c.DataTable[i].TargetMatchAnyRegexp = r
		}

		if c.DataTable[i].TargetMatchAll != "" {
			r, err := regexp.Compile(c.DataTable[i].TargetMatchAll)
			if err != nil {
				return err
			}

			c.DataTable[i].TargetMatchAllRegexp = r
		}

		rdp := c.DataTable[i].RollupDefaultPrecision
		rdf := c.DataTable[i].RollupDefaultFunction

		if c.DataTable[i].RollupConf == "auto" || c.DataTable[i].RollupConf == "" {
			table := c.DataTable[i].Table
			interval := time.Minute

			if c.DataTable[i].RollupAutoTable != "" {
				table = c.DataTable[i].RollupAutoTable
			}

			if c.DataTable[i].RollupAutoInterval != nil {
				interval = *c.DataTable[i].RollupAutoInterval
			}

			c.DataTable[i].Rollup, err = rollup.NewAuto(
				c.ClickHouse.URL,
				c.ClickHouse.TLSConfig,
				table,
				interval,
				rdp,
				rdf,
			)
		} else if c.DataTable[i].RollupConf == "none" {
			c.DataTable[i].Rollup, err = rollup.NewDefault(rdp, rdf)
		} else {
			c.DataTable[i].Rollup, err = rollup.NewXMLFile(c.DataTable[i].RollupConf, rdp, rdf)
		}

		if err != nil {
			return err
		}

		if len(c.DataTable[i].Context) == 0 {
			c.DataTable[i].ContextMap = knownDataTableContext
		} else {
			c.DataTable[i].ContextMap = make(map[string]bool)
			for _, ctx := range c.DataTable[i].Context {
				if !knownDataTableContext[ctx] {
					return fmt.Errorf("unknown context %#v", ctx)
				}

				c.DataTable[i].ContextMap[ctx] = true
			}
		}
	}

	return nil
}

func checkDeprecations(cfg *Config, d map[string]error) {
	if cfg.ClickHouse.DataTableLegacy != "" {
		d["data-table"] = fmt.Errorf("data-table parameter in [clickhouse] is deprecated; use [[data-table]]")
	}
}

func CreateCache(cacheName string, cacheConfig *CacheConfig) (cache.BytesCache, error) {
	if cacheConfig.DefaultTimeoutSec <= 0 && cacheConfig.ShortTimeoutSec <= 0 && cacheConfig.FindTimeoutSec <= 0 {
		return nil, nil
	}

	if cacheConfig.DefaultTimeoutSec < cacheConfig.ShortTimeoutSec {
		cacheConfig.DefaultTimeoutSec = cacheConfig.ShortTimeoutSec
	}

	if cacheConfig.ShortTimeoutSec < 0 || cacheConfig.DefaultTimeoutSec == cacheConfig.ShortTimeoutSec {
		// broken value or short timeout not need due to equal
		cacheConfig.ShortTimeoutSec = 0
	}

	if cacheConfig.DefaultTimeoutSec < cacheConfig.ShortTimeoutSec {
		cacheConfig.DefaultTimeoutSec = cacheConfig.ShortTimeoutSec
	}

	if cacheConfig.ShortDuration == 0 {
		cacheConfig.ShortDuration = 3 * time.Hour
	}

	if cacheConfig.ShortUntilOffsetSec == 0 {
		cacheConfig.ShortUntilOffsetSec = 120
	}

	cacheConfig.DefaultTimeoutStr = strconv.Itoa(int(cacheConfig.DefaultTimeoutSec))
	cacheConfig.ShortTimeoutStr = strconv.Itoa(int(cacheConfig.ShortTimeoutSec))

	switch cacheConfig.Type {
	case "memcache":
		if len(cacheConfig.MemcachedServers) == 0 {
			return nil, fmt.Errorf(cacheName + ": memcache cache requested but no memcache servers provided")
		}

		return cache.NewMemcached("gch-"+cacheName, cacheConfig.MemcachedServers...), nil
	case "mem":
		return cache.NewExpireCache(uint64(cacheConfig.Size * 1024 * 1024)), nil
	case "null":
		// defaults
		return nil, nil
	default:
		return nil, fmt.Errorf(
			"%s: unknown cache type '%s', known_cache_types 'null', 'mem', 'memcache'",
			cacheName,
			cacheConfig.Type,
		)
	}
}

func (c *Config) setupGraphiteMetrics() bool {
	if c.Metrics.MetricEndpoint == "" {
		metrics.DisableMetrics()
	} else {
		if c.Metrics.MetricInterval == 0 {
			c.Metrics.MetricInterval = 60 * time.Second
		}

		if c.Metrics.MetricTimeout == 0 {
			c.Metrics.MetricTimeout = time.Second
		}

		hostname, _ := os.Hostname()
		fqdn := strings.ReplaceAll(hostname, ".", "_")
		hostname = strings.Split(hostname, ".")[0]

		c.Metrics.MetricPrefix = strings.ReplaceAll(c.Metrics.MetricPrefix, "{prefix}", c.Metrics.MetricPrefix)
		c.Metrics.MetricPrefix = strings.ReplaceAll(c.Metrics.MetricPrefix, "{fqdn}", fqdn)
		c.Metrics.MetricPrefix = strings.ReplaceAll(c.Metrics.MetricPrefix, "{host}", hostname)

		// register our metrics with graphite
		metrics.Graphite = graphite.New(c.Metrics.MetricInterval, c.Metrics.MetricPrefix, c.Metrics.MetricEndpoint, c.Metrics.MetricTimeout)

		if c.Metrics.Statsd != "" && c.Metrics.ExtendedStat {
			var err error

			config := &statsd.ClientConfig{
				Address:       c.Metrics.Statsd,
				Prefix:        c.Metrics.MetricPrefix,
				ResInterval:   5 * time.Minute,
				UseBuffered:   true,
				FlushInterval: 300 * time.Millisecond,
			}

			metrics.Gstatsd, err = statsd.NewClientWithConfig(config)
			if err != nil {
				metrics.Gstatsd = metrics.NullSender{}

				fmt.Fprintf(os.Stderr, "statsd init: %v\n", err)
			}
		}

		metrics.InitMetrics(&c.Metrics, c.ClickHouse.FindMaxQueries > 0, c.ClickHouse.TagsMaxQueries > 0)
	}

	metrics.AutocompleteQMetric = metrics.InitQueryMetrics("tags", &c.Metrics)
	metrics.FindQMetric = metrics.InitQueryMetrics("find", &c.Metrics)

	for i := 0; i < len(c.DataTable); i++ {
		c.DataTable[i].QueryMetrics = metrics.InitQueryMetrics(c.DataTable[i].Table, &c.Metrics)
	}

	if c.ClickHouse.IndexTable != "" {
		metrics.InitQueryMetrics(c.ClickHouse.IndexTable, &c.Metrics)
	}

	if c.ClickHouse.TaggedTable != "" {
		metrics.InitQueryMetrics(c.ClickHouse.TaggedTable, &c.Metrics)
	}

	if c.ClickHouse.TagsCountTable != "" {
		metrics.InitQueryMetrics(c.ClickHouse.TagsCountTable, &c.Metrics)
	}

	return metrics.Graphite != nil
}

func (c *Config) GetUserFindLimiter(username string) limiter.ServerLimiter {
	if username != "" && len(c.ClickHouse.UserLimits) > 0 {
		if q, ok := c.ClickHouse.UserLimits[username]; ok {
			return q.Limiter
		}
	}

	return c.ClickHouse.FindLimiter
}

func (c *Config) GetUserTagsLimiter(username string) limiter.ServerLimiter {
	if username != "" && len(c.ClickHouse.UserLimits) > 0 {
		if q, ok := c.ClickHouse.UserLimits[username]; ok {
			return q.Limiter
		}
	}

	return c.ClickHouse.TagsLimiter
}

// search on sorted slice
func GetQueryParam(a []QueryParam, duration time.Duration) int {
	if indx := binarySearchQueryParamLe(a, duration, 0, len(a)); indx == -1 {
		return 0
	} else {
		return indx
	}
}
