package main

import (
	"bufio"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/lomik/graphite-clickhouse/helper/client"
)

func load(file string) (metricsFinds []string, tagsAutocompletes []string, renders []string, err error) {
	inFile, err := os.Open(file)
	if err != nil {
		return
	}
	defer inFile.Close()

	scanner := bufio.NewScanner(inFile)
	var ()
	for scanner.Scan() {
		query := scanner.Text()
		if strings.HasPrefix(query, "&query=") {
			metricsFinds = append(metricsFinds, query[1:])
		} else if strings.HasPrefix(query, "/tags/autoComplete/") {
			tagsAutocompletes = append(tagsAutocompletes, query)
		} else if strings.HasPrefix(query, "&target=") {
			renders = append(renders, query[1:])
		} else if query != "" && !strings.HasPrefix(query, "#") {
			fmt.Fprintf(os.Stderr, "UNKNOWN: %s\n", query)
		}
	}
	return
}

// mean gets the average of a slice of numbers
func mean(input []int64) (m int64) {

	if len(input) == 0 {
		return
	}

	for _, n := range input {
		m += n
	}

	return int64(math.Round(float64(m) / float64(len(input))))
}

// percentile of time.Durations against sorted slice
func percentile(input []int64, percent float64) (percentile int64, index int) {
	length := len(input)
	if length == 0 {
		return
	}

	if percent <= 0 || percent > 1 {
		return
	}

	if length == 1 {
		return input[0], 0
	}

	// Multiply percent by length of input
	indexF := percent * float64(length)

	// Check if the index is a whole number
	if indexF == float64(int64(index)) {

		// Convert float to int
		index = int(index) - 1

		// Find the value at the index
		percentile = input[index]

	} else if index > 1 {

		// Convert float to int via truncation
		index = int(index) - 1

		// Find the average of the index and following values
		percentile = (input[index] + input[index+1]) / 2

	} else {
		return 0, 0
	}

	return

}

func aggDuration(results []int64) time.Duration {
	if len(results) == 0 {
		return 0
	}
	if len(results) == 1 {
		return time.Duration(results[0])
	}
	sort.Slice(results, func(i, j int) bool { return results[i] < results[j] })
	if len(results) == 2 {
		return time.Duration(results[0])
	}
	if len(results) == 3 {
		return time.Duration(results[1])
	}
	if len(results) == 4 {
		results = results[1:3]
	} else {
		_, start := percentile(results, 0.25)
		_, end := percentile(results, 0.95)
		if start == 0 {
			start = 1
		}
		if end == 0 {
			end = len(results)
		}
		results = results[start:end]
	}

	return time.Duration(mean(results))
}

func metricsFindQueries(httpClient *http.Client, address string, format client.FormatType, metricsFinds []string, from, until int64, n uint) {
	fmt.Print("#query\tduration\tresp length\treq id\terror\n")
	if n < 1 {
		n = 2
	}
	results := make([]int64, 0, n)
	var (
		r          []client.FindMatch
		respHeader http.Header
	)
	for _, query := range metricsFinds {
		q := strings.Split(query, "&")
		for _, query := range q {
			if strings.HasPrefix(query, "query=") {
				mFind, err := url.QueryUnescape(query[6:])
				if err == nil {
					for i := uint(0); i < n; i++ {
						start := time.Now()
						_, r, respHeader, err = client.MetricsFind(httpClient, address, format, mFind, from, until, true)
						d := time.Since(start).Nanoseconds()
						results = append(results, d)
						if err != nil {
							break
						}
					}
					d := aggDuration(results)
					if err == nil {
						fmt.Print(query + "\t" + d.String() + "\t" + strconv.Itoa(len(r)) + "\t" +
							respHeader.Get("X-Gch-Request-Id") + "\t\n")
					} else {
						fmt.Print(query + "\t" + d.String() + "\t" + strconv.Itoa(len(r)) + "\t" +
							respHeader.Get("X-Gch-Request-Id") + "\t\"" + err.Error() + "\"")
					}
				} else {
					fmt.Fprintf(os.Stderr, "ERROR: %s\n", query)
				}
			}
		}
	}
}

func tagsAutocompleteQueries(httpClient *http.Client, address string, format client.FormatType, tagsAutocompletes []string, limit uint64, from, until int64, n uint) {
	if n < 1 {
		n = 2
	}
	results := make([]int64, 0, n)
	var (
		r          []string
		respHeader http.Header
		err        error
	)
	fmt.Print("#tags/autoComplete\tduration\tresp length\treq id\terror\n")
	for _, query := range tagsAutocompletes {
		for i := uint(0); i < n; i++ {
			start := time.Now()
			_, r, respHeader, err = client.TagsNames(httpClient, address, format, query, limit, from, until, true)
			d := time.Since(start).Nanoseconds()
			results = append(results, d)
			if err != nil {
				break
			}
		}
		d := aggDuration(results)
		if err == nil {
			fmt.Print(query + "\t" + d.String() + "\t" + strconv.Itoa(len(r)) + "\t" +
				respHeader.Get("X-Gch-Request-Id") + "\t\n")
		} else {
			fmt.Print(query + "\t" + d.String() + "\t" + strconv.Itoa(len(r)) + "\t" +
				respHeader.Get("X-Gch-Request-Id") + "\t" + err.Error())
		}
	}
}

func renderQueries(httpClient *http.Client, address string, format client.FormatType, renders []string, from, until int64, n uint) {
	if n < 1 {
		n = 2
	}
	results := make([]int64, 0, n)
	var (
		r          []client.Metric
		respHeader http.Header
		err        error
	)
	fmt.Print("#targets\tduration\tresp length\treq id\terror\n")
LOOP:
	for _, query := range renders {
		q := strings.Split(query, "&")
		renderTargets := make([]string, 0, len(q))
		for _, v := range q {
			if strings.HasPrefix(v, "target=") {
				v, err = url.QueryUnescape(v[7:])
				if err != nil {
					fmt.Fprintf(os.Stderr, "ERROR: %s\n", query)
					continue LOOP
				}
				renderTargets = append(renderTargets, v)
			}
		}
		for i := uint(0); i < n; i++ {
			start := time.Now()
			_, r, respHeader, err = client.Render(httpClient, address, format, renderTargets, from, until, true)
			d := time.Since(start).Nanoseconds()
			results = append(results, d)
			if err != nil {
				break
			}
		}
		d := aggDuration(results)
		if err == nil {
			fmt.Print(query + "\t" + d.String() + "\t" + strconv.Itoa(len(r)) + "\t" +
				respHeader.Get("X-Gch-Request-Id") + "\t\n")
		} else {
			fmt.Print(query + "\t" + d.String() + "\t" + strconv.Itoa(len(r)) + "\t" +
				respHeader.Get("X-Gch-Request-Id") + "\t" + err.Error())
		}
	}
}

type Stat struct {
	query    string
	duration time.Duration
	respLen  int
	reqId    string
	error    string
}

// "#tags/autoComplete\tduration\tresp length\treq id\terror\n"
func LoadStat(file string, stat *[]Stat) (name string, err error) {
	inFile, err := os.Open(file)
	if err != nil {
		return
	}
	defer inFile.Close()

	var (
		record   []string
		duration time.Duration
		respLen  int
	)

	r := csv.NewReader(inFile)
	r.Comma = '\t'
	record, err = r.Read()
	if err != nil {
		return
	}
	name = record[0]
	if (name != "#query" && !strings.HasPrefix(name, "#tags/") && name != "#targets") || len(record) != 5 {
		return name, errors.New("invalid header")
	}

	for {
		record, err = r.Read()
		if err == io.EOF {
			err = nil
			break
		} else if err != nil {
			break
		}
		query := record[0]
		if duration, err = time.ParseDuration(record[1]); err != nil {
			fmt.Fprintf(os.Stderr, "WARNING: duration parse [%d] (%s)\n", len(record)+1, record[0])
			continue
		}
		if respLen, err = strconv.Atoi(record[2]); err != nil {
			fmt.Fprintf(os.Stderr, "WARNING: resp len parse [%d] (%s)\n", len(record)+1, record[1])
			continue
		}
		reqId := record[3]
		error := record[4]

		*stat = append(*stat, Stat{
			query:    query,
			duration: duration,
			respLen:  respLen,
			reqId:    reqId,
			error:    error,
		})
	}

	return
}

type StatDiff struct {
	query        string
	duration     time.Duration
	durationDiff float64
	respLen      int
	respLenDiff  int
	error        string
	errorOld     string
}

func compareStat(stat []Stat, statOldMap map[string]Stat) (diff []StatDiff) {
	diff = make([]StatDiff, 0, len(stat))
	for i := 0; i < len(stat); i++ {
		if s, ok := statOldMap[stat[i].query]; ok {
			diff = append(diff, StatDiff{
				query:        s.query,
				duration:     stat[i].duration,
				durationDiff: 100 * float64(stat[i].duration.Nanoseconds()-s.duration.Nanoseconds()) / float64(s.duration.Nanoseconds()),
				respLen:      stat[i].respLen,
				respLenDiff:  stat[i].respLen - s.respLen,
				error:        stat[i].error,
				errorOld:     stat[i].error,
			})
		}
	}
	return
}
