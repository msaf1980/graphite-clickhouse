package render

import (
	"fmt"
	"net/http"
	"time"

	"go.uber.org/zap"

	"github.com/go-graphite/carbonapi/pkg/parser"

	"github.com/lomik/graphite-clickhouse/config"
	"github.com/lomik/graphite-clickhouse/helper/clickhouse"
	"github.com/lomik/graphite-clickhouse/helper/finder"
	"github.com/lomik/graphite-clickhouse/limiter"
	"github.com/lomik/graphite-clickhouse/logs"
	"github.com/lomik/graphite-clickhouse/metrics"
	"github.com/lomik/graphite-clickhouse/pkg/scope"
	"github.com/lomik/graphite-clickhouse/render/data"
	"github.com/lomik/graphite-clickhouse/render/reply"
)

// Handler serves /render requests
type Handler struct {
	config *config.Config
}

// NewHandler generates new *Handler
func NewHandler(config *config.Config) *Handler {
	h := &Handler{
		config: config,
	}

	return h
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var (
		maxDuration   int64
		targetsLen    int
		metricsLen    int
		pointsCount   int64
		fetchStart    time.Time
		cachedFind    bool
		queueFail     bool
		queueDuration time.Duration
		err           error
		fetchRequests data.MultiTarget
		luser         string
	)
	start := time.Now()
	status := http.StatusOK
	accessLogger := scope.LoggerWithHeaders(r.Context(), r, h.config.Common.HeadersToLog).Named("http")
	logger := scope.LoggerWithHeaders(r.Context(), r, h.config.Common.HeadersToLog).Named("render")

	r = r.WithContext(scope.WithLogger(r.Context(), logger))

	username := r.Header.Get("X-Forwarded-User")
	var qlimiter limiter.ServerLimiter = limiter.NoopLimiter{}

	defer func() {
		if rec := recover(); rec != nil {
			status = http.StatusInternalServerError
			logger.Error("panic during eval:",
				zap.String("requestID", scope.String(r.Context(), "requestID")),
				zap.Any("reason", rec),
				zap.Stack("stack"),
			)
			answer := fmt.Sprintf("%v\nStack trace: %v", rec, zap.Stack("").String)
			http.Error(w, answer, status)
		}
		end := time.Now()
		logs.AccessLog(accessLogger, h.config, r, status, end.Sub(start), queueDuration, cachedFind, queueFail)
		qlimiter.SendDuration(queueDuration.Milliseconds())
		metrics.SendRenderMetrics(metrics.RenderRequestMetric, status, start, fetchStart, end, maxDuration, h.config.Metrics.ExtendedStat, int64(metricsLen), pointsCount)
	}()

	r.ParseMultipartForm(1024 * 1024)
	formatter, err := reply.GetFormatter(r)
	if err != nil {
		status = http.StatusBadRequest
		logger.Error("formatter", zap.Error(err))
		http.Error(w, fmt.Sprintf("Failed to parse request: %v", err.Error()), status)
		return
	}

	fetchRequests, err = formatter.ParseRequest(r)
	if err != nil {
		status = http.StatusBadRequest
		http.Error(w, fmt.Sprintf("Failed to parse request: %v", err.Error()), status)
		return
	}
	for tf, targets := range fetchRequests {
		if tf.From >= tf.Until {
			// wrong duration
			if err != nil {
				status, _ = clickhouse.HandleError(w, clickhouse.ErrInvalidTimeRange)
				return
			}
		}
		targetsLen += len(targets.List)
	}

	luser, qlimiter = data.GetQueryLimiter(username, h.config, &fetchRequests)
	logger.Debug("use user limiter", zap.String("username", username), zap.String("luser", luser))

	var maxCacheTimeoutStr string
	useCache := h.config.Common.FindCache != nil && !parser.TruthyBool(r.FormValue("noCache"))

	if useCache {
		var cached int
		cached, maxCacheTimeoutStr, err = finder.FinderCached(h.config, start, fetchRequests, logger, &metricsLen)
		if err != nil {
			status, _ = clickhouse.HandleError(w, err)
			return
		}
		if cached > 0 {
			if cached == targetsLen && metricsLen == 0 {
				// all from cache and no metric
				status = http.StatusNotFound
				formatter.Reply(w, r, data.EmptyResponse())
				return
			}
			cachedFind = true
		}
	}

	maxDuration, err = finder.Find(h.config, fetchRequests, r.Context(), logger, qlimiter, &metricsLen, &queueDuration, useCache)
	if err != nil {
		status, queueFail = clickhouse.HandleError(w, err)
		return
	}

	logger.Info("finder", zap.Int("metrics", metricsLen), zap.Bool("find_cached", cachedFind))

	if cachedFind {
		w.Header().Set("X-Cached-Find", maxCacheTimeoutStr)
	}
	if metricsLen == 0 {
		status = http.StatusNotFound
		formatter.Reply(w, r, data.EmptyResponse())
		return
	}

	fetchStart = time.Now()

	reply, err := fetchRequests.Fetch(r.Context(), h.config, config.ContextGraphite, qlimiter, &queueDuration)
	if err != nil {
		status, queueFail = clickhouse.HandleError(w, err)
		return
	}

	if len(reply) == 0 {
		status = http.StatusNotFound
		formatter.Reply(w, r, reply)
		return
	}

	for i := range reply {
		pointsCount += int64(reply[i].Data.Len())
	}
	rStart := time.Now()
	formatter.Reply(w, r, reply)
	d := time.Since(rStart)
	logger.Debug("reply", zap.String("runtime", d.String()), zap.Duration("runtime_ns", d))
}
