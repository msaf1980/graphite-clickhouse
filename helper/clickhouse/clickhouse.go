package clickhouse

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/lomik/graphite-clickhouse/helper/errs"
	httpHelper "github.com/lomik/graphite-clickhouse/helper/http"
	"github.com/lomik/graphite-clickhouse/limiter"
	"github.com/lomik/graphite-clickhouse/pkg/scope"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type ErrWithDescr struct {
	err  string
	data string
}

type ContentEncoding string

const (
	ContentEncodingNone ContentEncoding = "none"
	ContentEncodingGzip ContentEncoding = "gzip"
	ContentEncodingZstd ContentEncoding = "zstd"
)

const (
	ClickHouseProgressHeader string = "X-Clickhouse-Progress"
	ClickHouseSummaryHeader  string = "X-Clickhouse-Summary"
)

func NewErrWithDescr(err string, data string) error {
	return &ErrWithDescr{err, data}
}

func (e *ErrWithDescr) Error() string {
	return e.err + ": " + e.data
}

func (e *ErrWithDescr) PrependDescription(test string) {
	e.data = test + e.data
}

var ErrInvalidTimeRange = errors.New("Invalid or empty time range")
var ErrUvarintRead = errors.New("ReadUvarint: Malformed array")
var ErrUvarintOverflow = errors.New("ReadUvarint: varint overflows a 64-bit integer")
var ErrClickHouseResponse = errors.New("Malformed response from clickhouse")

func extractClickhouseError(e string) (int, string) {
	if strings.HasPrefix(e, "clickhouse response status 500: Code:") || strings.HasPrefix(e, "Malformed response from clickhouse") {
		if start := strings.Index(e, ": Limit for "); start != -1 {
			e := e[start+8:]
			if end := strings.Index(e, " (version "); end != -1 {
				e = e[0:end]
			}

			return http.StatusForbidden, "Storage read limit " + e
		} else if start := strings.Index(e, ": Memory limit "); start != -1 {
			return http.StatusForbidden, "Storage read limit for memory"
		} else if strings.HasPrefix(e, "clickhouse response status 500: Code: 170,") {
			// distributed table configuration error
			// clickhouse response status 500: Code: 170, e.displayText() = DB::Exception: Requested cluster 'cluster' not found
			return http.StatusServiceUnavailable, "Storage configuration error"
		}
	}

	if strings.HasPrefix(e, "clickhouse response status 404: Code: 60. DB::Exception: Table default.") {
		return http.StatusServiceUnavailable, "Storage default tables damaged"
	}

	if strings.HasPrefix(e, "clickhouse response status 500: Code: 427") || strings.HasPrefix(e, "clickhouse response status 400: Code: 427.") {
		return http.StatusBadRequest, "Incorrect regex syntax"
	}

	return http.StatusServiceUnavailable, "Storage unavailable"
}

func HandleError(w http.ResponseWriter, err error) (status int, queueFail bool) {
	status = http.StatusOK
	errStr := err.Error()

	if err == ErrInvalidTimeRange {
		status = http.StatusBadRequest
		http.Error(w, errStr, status)

		return
	}

	if err == limiter.ErrTimeout || err == limiter.ErrOverflow {
		queueFail = true
		status = http.StatusServiceUnavailable
		http.Error(w, err.Error(), status)

		return
	}

	if _, ok := err.(*ErrWithDescr); ok {
		status, errStr = extractClickhouseError(errStr)
		http.Error(w, errStr, status)

		return
	}

	netErr, ok := err.(net.Error)
	if ok {
		if netErr.Timeout() {
			status = http.StatusGatewayTimeout
			http.Error(w, "Storage read timeout", status)
		} else if strings.HasSuffix(errStr, "connect: no route to host") ||
			strings.HasPrefix(errStr, "dial tcp: lookup ") { // DNS lookup
			status = http.StatusServiceUnavailable
			http.Error(w, "Storage route error", status)
		} else if strings.HasSuffix(errStr, "connect: connection refused") ||
			strings.HasSuffix(errStr, ": connection reset by peer") {
			status = http.StatusServiceUnavailable
			http.Error(w, "Storage connect error", status)
		} else {
			status = http.StatusServiceUnavailable
			http.Error(w, "Storage network error", status)
		}

		return
	}

	errCode, ok := err.(errs.ErrorWithCode)
	if ok {
		if (errCode.Code > 500 && errCode.Code < 512) ||
			errCode.Code == http.StatusBadRequest || errCode.Code == http.StatusForbidden {
			status = errCode.Code
			http.Error(w, html.EscapeString(errStr), status)
		} else {
			status = http.StatusInternalServerError
			http.Error(w, html.EscapeString(errStr), status)
		}

		return
	}

	if errors.Is(err, context.Canceled) {
		status = http.StatusGatewayTimeout
		http.Error(w, "Storage read context canceled", status)
	} else {
		//logger.Debug("query", zap.Error(err))
		status = http.StatusInternalServerError
		http.Error(w, html.EscapeString(errStr), status)
	}

	return
}

type Options struct {
	TLSConfig               *tls.Config
	Timeout                 time.Duration
	ConnectTimeout          time.Duration
	ProgressSendingInterval time.Duration
	CheckRequestProgress    bool
}

type LoggedReader struct {
	reader     io.ReadCloser
	logger     *zap.Logger
	start      time.Time
	finished   bool
	queryID    string
	read_rows  int64
	read_bytes int64
}

func (r *LoggedReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	if err != nil && !r.finished {
		r.finished = true
		r.logger.Info("query", zap.String("query_id", r.queryID), zap.Duration("time", time.Since(r.start)))
	}

	return n, err
}

func (r *LoggedReader) Close() error {
	err := r.reader.Close()

	if !r.finished {
		r.finished = true
		r.logger.Info("query", zap.String("query_id", r.queryID), zap.Duration("time", time.Since(r.start)))
	}

	return err
}

func (r *LoggedReader) ChReadRows() int64 {
	return r.read_rows
}

func (r *LoggedReader) ChReadBytes() int64 {
	return r.read_bytes
}

type queryStats struct {
	readRows     int64
	readBytes    int64
	loggerFields []zapcore.Field
	rawHeader    string
}

func formatSQL(q string) string {
	s := strings.Split(q, "\n")
	for i := 0; i < len(s); i++ {
		s[i] = strings.TrimSpace(s[i])
	}

	return strings.Join(s, " ")
}

func Query(ctx context.Context, dsn string, query string, opts Options, extData *ExternalData) ([]byte, int64, int64, error) {
	return Post(ctx, dsn, query, nil, opts, extData)
}

func Post(ctx context.Context, dsn string, query string, postBody io.Reader, opts Options, extData *ExternalData) ([]byte, int64, int64, error) {
	return do(ctx, dsn, query, postBody, ContentEncodingNone, opts, extData)
}

// Deprecated: use PostWithEncoding instead
func PostGzip(ctx context.Context, dsn string, query string, postBody io.Reader, opts Options, extData *ExternalData) ([]byte, int64, int64, error) {
	return do(ctx, dsn, query, postBody, ContentEncodingGzip, opts, extData)
}

func PostWithEncoding(ctx context.Context, dsn string, query string, postBody io.Reader, encoding ContentEncoding, opts Options, extData *ExternalData) ([]byte, int64, int64, error) {
	return do(ctx, dsn, query, postBody, encoding, opts, extData)
}

func Reader(ctx context.Context, dsn string, query string, opts Options, extData *ExternalData) (*LoggedReader, error) {
	return reader(ctx, dsn, query, nil, ContentEncodingNone, opts, extData)
}

func reader(ctx context.Context, dsn string, query string, postBody io.Reader, encoding ContentEncoding, opts Options, extData *ExternalData) (bodyReader *LoggedReader, err error) {
	if postBody != nil && extData != nil {
		err = fmt.Errorf("postBody and extData could not be passed in one request")
		return
	}

	var chQueryID string

	start := time.Now()

	requestID := scope.RequestID(ctx)

	queryForLogger := query
	if len(queryForLogger) > 500 {
		queryForLogger = queryForLogger[:395] + "<...>" + queryForLogger[len(queryForLogger)-100:]
	}

	logger := scope.Logger(ctx).With(zap.String("query", formatSQL(queryForLogger)))

	defer func() {
		// fmt.Println(time.Since(start), formatSQL(queryForLogger))
		if err != nil {
			logger.Error("query", zap.Error(err), zap.Duration("time", time.Since(start)))
		}
	}()

	p, err := url.Parse(dsn)
	if err != nil {
		return
	}

	var b [8]byte

	binary.LittleEndian.PutUint64(b[:], rand.Uint64())
	queryID := fmt.Sprintf("%x", b)

	q := p.Query()
	q.Set("query_id", fmt.Sprintf("%s::%s", requestID, queryID))
	// Get X-Clickhouse-Summary header
	// TODO: remove when https://github.com/ClickHouse/ClickHouse/issues/16207 is done
	q.Set("send_progress_in_http_headers", "1")
	q.Set("http_headers_progress_interval_ms", strconv.FormatInt(opts.ProgressSendingInterval.Milliseconds(), 10))
	p.RawQuery = q.Encode()

	var contentHeader string

	if postBody != nil {
		q := p.Query()
		q.Set("query", query)
		p.RawQuery = q.Encode()
	} else if extData != nil {
		q := p.Query()
		q.Set("query", query)
		p.RawQuery = q.Encode()

		postBody, contentHeader, err = extData.buildBody(ctx, p)
		if err != nil {
			return
		}
	} else {
		postBody = strings.NewReader(query)
	}

	url := p.String()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, postBody)
	if err != nil {
		return
	}

	req.Header.Add("User-Agent", scope.ClickhouseUserAgent(ctx))

	if contentHeader != "" {
		req.Header.Add("Content-Type", contentHeader)
	}

	switch encoding {
	case ContentEncodingNone:
		// no encoding
	case ContentEncodingGzip:
		req.Header.Add("Content-Encoding", "gzip")
	case ContentEncodingZstd:
		req.Header.Add("Content-Encoding", "zstd")
	default:
		return nil, fmt.Errorf("unknown encoding: %s", encoding)
	}

	var resp *http.Response
	if opts.CheckRequestProgress {
		resp, err = sendRequestWithProgressCheck(req, &opts)
	} else {
		resp, err = sendRequestViaDefaultClient(req, &opts)
	}

	if err != nil {
		if opts.CheckRequestProgress && resp != nil {
			stats, parse_err := getQueryStats(resp, ClickHouseProgressHeader)
			if parse_err != nil {
				logger.Warn("query", zap.Error(err), zap.String("clickhouse-progress", stats.rawHeader))
			}

			logger = logger.With(stats.loggerFields...)
		}

		return
	}

	// chproxy overwrite our query id. So read it again
	chQueryID = resp.Header.Get("X-ClickHouse-Query-Id")

	stats, err := getQueryStats(resp, ClickHouseSummaryHeader)
	if err != nil {
		summaryHeader := resp.Header.Get(ClickHouseSummaryHeader)
		logger.Warn("query",
			zap.Error(err),
			zap.String("clickhouse-summary", summaryHeader))

		err = nil
	}

	read_rows, read_bytes, fields := stats.readRows, stats.readBytes, stats.loggerFields

	if len(fields) > 0 {
		sort.Slice(fields, func(i, j int) bool {
			return fields[i].Key < fields[j].Key
		})

		logger = logger.With(fields...)
	}

	// check for return 5xx error, may be 502 code if clickhouse accesed via reverse proxy
	if resp.StatusCode > http.StatusInternalServerError && resp.StatusCode < 512 {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		err = errs.NewErrorWithCode(string(body), resp.StatusCode)

		return
	} else if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		err = NewErrWithDescr("clickhouse response status "+strconv.Itoa(resp.StatusCode), string(body))

		return
	}

	bodyReader = &LoggedReader{
		reader:     resp.Body,
		logger:     logger,
		start:      start,
		queryID:    chQueryID,
		read_rows:  read_rows,
		read_bytes: read_bytes,
	}

	return
}

func getQueryStats(resp *http.Response, statsHeaderName string) (queryStats, error) {
	read_rows := int64(-1)
	read_bytes := int64(-1)

	if resp == nil {
		return queryStats{
			readRows:     read_rows,
			readBytes:    read_bytes,
			loggerFields: []zapcore.Field{},
		}, nil
	}

	statsHeader := ""
	statsHeaders := resp.Header.Values(statsHeaderName)

	if len(statsHeaders) > 0 {
		statsHeader = statsHeaders[len(statsHeaders)-1]
	} else {
		return queryStats{
			readRows:     read_rows,
			readBytes:    read_bytes,
			loggerFields: []zapcore.Field{},
		}, nil
	}

	stats := make(map[string]string)

	err := json.Unmarshal([]byte(statsHeader), &stats)
	if err != nil {
		return queryStats{
			readRows:     read_rows,
			readBytes:    read_bytes,
			loggerFields: []zapcore.Field{},
			rawHeader:    statsHeader,
		}, err
	}

	// TODO: use in carbon metrics sender when it will be implemented
	fields := make([]zapcore.Field, 0, len(stats))
	for k, v := range stats {
		fields = append(fields, zap.String(k, v))

		switch k {
		case "read_rows":
			read_rows, _ = strconv.ParseInt(v, 10, 64)
		case "read_bytes":
			read_bytes, _ = strconv.ParseInt(v, 10, 64)
		}
	}

	sort.Slice(fields, func(i int, j int) bool {
		return fields[i].Key < fields[j].Key
	})

	return queryStats{
		readRows:     read_rows,
		readBytes:    read_bytes,
		loggerFields: fields,
		rawHeader:    statsHeader,
	}, nil
}

func sendRequestViaDefaultClient(request *http.Request, opts *Options) (*http.Response, error) {
	client := &http.Client{
		Timeout: opts.Timeout,
		Transport: &http.Transport{
			DialContext: (&net.Dialer{
				Timeout: opts.ConnectTimeout,
			}).DialContext,
			TLSClientConfig:   opts.TLSConfig,
			DisableKeepAlives: true,
		},
	}

	return client.Do(request)
}

func sendRequestWithProgressCheck(request *http.Request, opts *Options) (*http.Response, error) {
	transport := &http.Transport{
		DialContext: (&net.Dialer{
			Timeout: opts.ConnectTimeout,
		}).DialContext,
		TLSClientConfig:   opts.TLSConfig,
		DisableKeepAlives: true,
	}

	return httpHelper.DoHTTPOverTCP(request.Context(), transport, request, opts.Timeout)
}

func do(ctx context.Context, dsn string, query string, postBody io.Reader, encoding ContentEncoding, opts Options, extData *ExternalData) ([]byte, int64, int64, error) {
	bodyReader, err := reader(ctx, dsn, query, postBody, encoding, opts, extData)
	if err != nil {
		return nil, 0, 0, err
	}

	body, err := io.ReadAll(bodyReader)
	bodyReader.Close()

	if err != nil {
		return nil, bodyReader.ChReadRows(), bodyReader.ChReadBytes(), err
	}

	return body, bodyReader.ChReadRows(), bodyReader.ChReadBytes(), nil
}

func ReadUvarint(array []byte) (uint64, int, error) {
	var x uint64

	var s uint

	l := len(array) - 1

	for i := 0; ; i++ {
		if i > l {
			return x, i + 1, ErrUvarintRead
		}

		if array[i] < 0x80 {
			if i > 9 || i == 9 && array[i] > 1 {
				return x, i + 1, ErrUvarintOverflow
			}

			return x | uint64(array[i])<<s, i + 1, nil
		}

		x |= uint64(array[i]&0x7f) << s
		s += 7
	}
}
