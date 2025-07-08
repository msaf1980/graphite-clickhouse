package client

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/msaf1980/go-stringutils"
)

// TagsNames do  /tags/autoComplete/tags request with query like [tagPrefix];tag1=value1;tag2=~value*
// Valid formats are json
type TagsNames struct {
	queryParams string
	format      FormatType
	u           *url.URL
	body        []byte
}

func NewTagsNames(address string, format FormatType, query string, limit uint64, from, until int64) (t TagsNames, err error) {
	rTags := "/tags/autoComplete/tags"

	if format == FormatDefault {
		format = FormatJSON
	}

	t.queryParams = fmt.Sprintf("%s?format=%s, from=%d, until=%d, limits=%d, query %s", rTags, format.String(), from, until, limit, query)
	t.format = format

	if format != FormatJSON {
		err = ErrUnsupportedFormat
		return
	}

	t.u, err = url.Parse(address + rTags)
	if err != nil {
		return
	}

	var tagPrefix string

	var exprs []string

	if query != "" && query != "<>" {
		args := strings.Split(query, ";")
		if len(args) < 1 {
			err = ErrInvalidQuery
			return
		}

		exprs = make([]string, 0, len(args))

		for i, arg := range args {
			delim := strings.IndexRune(arg, '=')
			if i == 0 && delim == -1 {
				tagPrefix = arg
			} else if delim <= 0 {
				err = errors.New("invalid expr: " + arg)
				return
			} else {
				exprs = append(exprs, arg)
			}
		}
	}

	v := make([]string, 0, 2+len(exprs))

	var rawQuery stringutils.Builder

	rawQuery.Grow(128)

	v = append(v, "format="+format.String())

	rawQuery.WriteString("format=")
	rawQuery.WriteString(url.QueryEscape(format.String()))

	if tagPrefix != "" {
		v = append(v, "tagPrefix="+tagPrefix)

		rawQuery.WriteString("&tagPrefix=")
		rawQuery.WriteString(url.QueryEscape(tagPrefix))
	}

	for _, expr := range exprs {
		v = append(v, "expr="+expr)

		rawQuery.WriteString("&expr=")
		rawQuery.WriteString(url.QueryEscape(expr))
	}

	if from > 0 {
		fromStr := strconv.FormatInt(from, 10)
		v = append(v, "from="+fromStr)

		rawQuery.WriteString("&from=")
		rawQuery.WriteString(fromStr)
	}

	if until > 0 {
		untilStr := strconv.FormatInt(until, 10)
		v = append(v, "until="+untilStr)

		rawQuery.WriteString("&until=")
		rawQuery.WriteString(untilStr)
	}

	if limit > 0 {
		limitStr := strconv.FormatUint(limit, 10)
		v = append(v, "limit="+limitStr)

		rawQuery.WriteString("&limit=")
		rawQuery.WriteString(limitStr)
	}

	t.queryParams = fmt.Sprintf("%s %q", rTags, v)

	t.u.RawQuery = rawQuery.String()

	return
}

func (t *TagsNames) QueryParams() string {
	return t.queryParams
}

func (t *TagsNames) Query(client *http.Client) ([]string, time.Duration, http.Header, error) {
	var duration time.Duration
	req, err := http.NewRequest(http.MethodGet, t.u.String(), nil)
	if err != nil {
		return nil, duration, nil, err
	}

	now := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		duration = time.Since(now)
		return nil, duration, nil, err
	}

	defer resp.Body.Close()

	b, err := io.ReadAll(resp.Body)
	duration = time.Since(now)
	if err != nil {
		return nil, duration, nil, err
	}

	if resp.StatusCode == http.StatusNotFound {
		return nil, duration, resp.Header, nil
	} else if resp.StatusCode != http.StatusOK {
		return nil, duration, resp.Header, NewHttpError(resp.StatusCode, string(b))
	}

	var values []string

	err = json.Unmarshal(b, &values)
	if err != nil {
		return nil, duration, resp.Header, errors.New(err.Error() + ": " + string(b))
	}

	return values, duration, resp.Header, nil
}

func QueryTagsNames(client *http.Client, address string, format FormatType, query string, limit uint64, from, until int64) (string, []string, time.Duration, http.Header, error) {
	tagsQuery, err := NewTagsNames(address, format, query, limit, from, until)
	if err != nil {
		return tagsQuery.QueryParams(), nil, 0, nil, err
	} else {
		r, duration, respHeader, err := tagsQuery.Query(client)
		return tagsQuery.QueryParams(), r, duration, respHeader, err
	}
}

// TagsValues do  /tags/autoComplete/values request with query like searchTag[=valuePrefix];tag1=value1;tag2=~value*
// Valid formats are json
type TagsValues struct {
	queryParams string
	format      FormatType
	u           *url.URL
	body        []byte
}

func NewTagsValues(address string, format FormatType, query string, limit uint64, from, until int64) (t TagsValues, err error) {
	rTags := "/tags/autoComplete/values"

	if format == FormatDefault {
		format = FormatJSON
	}

	t.format = format

	if format != FormatJSON {
		t.queryParams = fmt.Sprintf("%s?format=%s, from=%d, until=%d, limits=%d, query %s", rTags, format.String(), from, until, limit, query)
		err = ErrUnsupportedFormat
		return
	}

	t.u, err = url.Parse(address + rTags)
	if err != nil {
		return
	}

	var (
		tag         string
		valuePrefix string
		exprs       []string
	)

	if query != "" && query != "<>" {
		args := strings.Split(query, ";")
		if len(args) < 2 {
			err = ErrInvalidQuery
			return
		}

		vals := strings.Split(args[0], "=")
		tag = vals[0]

		if len(vals) > 2 {
			err = errors.New("invalid tag: " + args[0])
			return
		} else if len(vals) == 2 {
			valuePrefix = vals[1]
		}

		exprs = make([]string, 0, len(args)-1)

		for i := 1; i < len(args); i++ {
			expr := args[i]
			if strings.IndexRune(expr, '=') <= 0 {
				err = errors.New("invalid expr: " + expr)
				return
			}

			exprs = append(exprs, expr)
		}
	}

	v := make([]string, 0, 2+len(exprs))

	var rawQuery stringutils.Builder

	rawQuery.Grow(128)

	v = append(v, "format="+format.String())

	rawQuery.WriteString("format=")
	rawQuery.WriteString(url.QueryEscape(format.String()))

	if tag != "" {
		v = append(v, "tag="+tag)

		rawQuery.WriteString("&tag=")
		rawQuery.WriteString(url.QueryEscape(tag))
	}

	if valuePrefix != "" {
		v = append(v, "valuePrefix="+valuePrefix)

		rawQuery.WriteString("&valuePrefix=")
		rawQuery.WriteString(url.QueryEscape(valuePrefix))
	}

	for _, expr := range exprs {
		v = append(v, "expr="+expr)

		rawQuery.WriteString("&expr=")
		rawQuery.WriteString(url.QueryEscape(expr))
	}

	if from > 0 {
		fromStr := strconv.FormatInt(from, 10)
		v = append(v, "from="+fromStr)

		rawQuery.WriteString("&from=")
		rawQuery.WriteString(fromStr)
	}

	if until > 0 {
		untilStr := strconv.FormatInt(until, 10)
		v = append(v, "until="+untilStr)

		rawQuery.WriteString("&until=")
		rawQuery.WriteString(untilStr)
	}

	if limit > 0 {
		limitStr := strconv.FormatUint(limit, 10)
		v = append(v, "limit="+limitStr)

		rawQuery.WriteString("&limit=")
		rawQuery.WriteString(limitStr)
	}

	t.queryParams = fmt.Sprintf("%s %q", rTags, v)

	t.u.RawQuery = rawQuery.String()

	return
}

func (t *TagsValues) QueryParams() string {
	return t.queryParams
}

func (t *TagsValues) Query(client *http.Client) ([]string, time.Duration, http.Header, error) {
	var duration time.Duration

	req, err := http.NewRequest(http.MethodGet, t.u.String(), nil)
	if err != nil {
		return nil, duration, nil, err
	}

	start := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		duration = time.Since(start)
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

	var values []string

	err = json.Unmarshal(b, &values)
	if err != nil {
		return nil, duration, resp.Header, errors.New(err.Error() + ": " + string(b))
	}

	return values, duration, resp.Header, nil
}

func QueryTagsValues(client *http.Client, address string, format FormatType, query string, limit uint64, from, until int64) (string, []string, time.Duration, http.Header, error) {
	tagsQuery, err := NewTagsValues(address, format, query, limit, from, until)
	if err != nil {
		return tagsQuery.QueryParams(), nil, 0, nil, err
	} else {
		r, duration, respHeader, err := tagsQuery.Query(client)
		return tagsQuery.QueryParams(), r, duration, respHeader, err
	}
}
