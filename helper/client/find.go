package client

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	protov2 "github.com/go-graphite/protocol/carbonapi_v2_pb"
	protov3 "github.com/go-graphite/protocol/carbonapi_v3_pb"

	pickle "github.com/lomik/og-rek"
)

type FindMatch struct {
	Path   string `toml:"path"`
	IsLeaf bool   `toml:"is_leaf"`
}

// MetricsFind do /metrics/find/ request
// Valid formats are carbonapi_v3_pb. protobuf, pickle
type MetricsFind struct {
	queryParams string
	format      FormatType
	u           *url.URL
	body        []byte
}

func NewMetricsFind(address string, format FormatType, query string, from, until int64) (m MetricsFind, err error) {
	if format == FormatDefault {
		format = FormatPb_v3
	}

	rUrl := "/metrics/find/"

	m.format = format
	m.queryParams = fmt.Sprintf("%s?format=%s, from=%d, until=%d, query %s", rUrl, format.String(), from, until, query)

	m.u, err = url.Parse(address + rUrl)
	if err != nil {
		return
	}

	v := url.Values{
		"format": []string{format.String()},
	}

	switch format {
	case FormatPb_v3:

		r := protov3.MultiGlobRequest{
			Metrics:   []string{query},
			StartTime: from,
			StopTime:  until,
		}

		m.body, err = r.Marshal()
		if err != nil {
			return
		}

	case FormatProtobuf, FormatPickle:
		v["query"] = []string{query}
		if from > 0 {
			v["from"] = []string{strconv.FormatInt(from, 10)}
		}

		if until > 0 {
			v["until"] = []string{strconv.FormatInt(until, 10)}
		}
	default:
		err = ErrUnsupportedFormat
		return
	}

	m.u.RawQuery = v.Encode()

	return
}

func (m *MetricsFind) QueryParams() string {
	return m.queryParams
}

func (m *MetricsFind) Query(client *http.Client) ([]FindMatch, time.Duration, http.Header, error) {

	var reader io.Reader
	var duration time.Duration

	if m.body != nil {
		reader = bytes.NewReader(m.body)
	}

	req, err := http.NewRequest(http.MethodGet, m.u.String(), reader)
	if err != nil {
		return nil, duration, nil, err
	}

	start := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		return nil, duration, nil, err
	}

	defer resp.Body.Close()

	b, err := io.ReadAll(resp.Body)
	duration = time.Since(start)
	if err != nil {
		return nil, duration, nil, err
	}

	if resp.StatusCode == http.StatusNotFound {
		return nil, duration, resp.Header, nil
	} else if resp.StatusCode != http.StatusOK {
		return nil, duration, resp.Header, NewHttpError(resp.StatusCode, string(b))
	}

	var globs []FindMatch

	switch m.format {
	case FormatProtobuf:
		var globsv2 protov2.GlobResponse
		if err = globsv2.Unmarshal(b); err != nil {
			return nil, duration, resp.Header, err
		}

		for _, m := range globsv2.Matches {
			globs = append(globs, FindMatch{Path: m.Path, IsLeaf: m.IsLeaf})
		}
	case FormatPb_v3:
		var globsv3 protov3.MultiGlobResponse
		if err = globsv3.Unmarshal(b); err != nil {
			return nil, duration, resp.Header, err
		}

		for _, m := range globsv3.Metrics {
			for _, v := range m.Matches {
				globs = append(globs, FindMatch{Path: v.Path, IsLeaf: v.IsLeaf})
			}
		}
	case FormatPickle:
		reader := bytes.NewReader(b)
		decoder := pickle.NewDecoder(reader)

		p, err := decoder.Decode()
		if err != nil {
			return nil, duration, resp.Header, err
		}

		for _, v := range p.([]interface{}) {
			m := v.(map[interface{}]interface{})
			path := m["metric_path"].(string)
			isLeaf := m["isLeaf"].(bool)
			globs = append(globs, FindMatch{Path: path, IsLeaf: isLeaf})
		}
	default:
		return nil, duration, resp.Header, ErrUnsupportedFormat
	}

	return globs, duration, resp.Header, nil
}

func QueryMetricsFind(client *http.Client, address string, format FormatType, query string, from, until int64) (string, []FindMatch, time.Duration, http.Header, error) {
	metricQuery, err := NewMetricsFind(address, format, query, from, until)
	if err != nil {
		return metricQuery.QueryParams(), nil, 0, nil, err
	} else {
		r, duration, respHeader, err := metricQuery.Query(client)
		return metricQuery.QueryParams(), r, duration, respHeader, err
	}
}
