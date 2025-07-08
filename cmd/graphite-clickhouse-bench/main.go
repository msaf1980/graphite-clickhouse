package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/go-graphite/protocol/carbonapi_v3_pb"
	"github.com/lomik/graphite-clickhouse/helper/client"
	"github.com/lomik/graphite-clickhouse/helper/datetime"
	"github.com/lomik/graphite-clickhouse/helper/utils"
)

func main() {
	address := flag.String("address", "http://127.0.0.1:9090", "Address of graphite-clickhouse server")
	fromStr := flag.String("from", "0", "from")
	untilStr := flag.String("until", "", "until")
	maxDataPointsStr := flag.String("maxDataPoints", "1048576", "Maximum amount of datapoints in response")

	metricsFind := flag.String("find", "", "Query for /metrics/find/ , valid formats are carbonapi_v3_pb. protobuf, pickle")

	tagsValues := flag.String("tags_values", "", "Query for /tags/autoComplete/values (with query like 'searchTag[=valuePrefix];tag1=value1;tag2=~value*' or '<>' for empty)")
	tagsNames := flag.String("tags_names", "", "Query for /tags/autoComplete/tags (with query like '[tagPrefix];tag1=value1;tag2=~value*[' or '<>' for empty)")
	limit := flag.Uint64("limit", 0, "limit for some queries (tags_values, tags_values)")

	var renderTargets utils.StringSlice

	flag.Var(&renderTargets, "target", "Target for /render")

	timeout := flag.Duration("timeout", time.Minute, "request timeout")

	format := client.FormatDefault
	flag.Var(&format, "format", fmt.Sprintf("Response format %v", client.FormatTypes()))

	flag.Parse()

	ec := 0

	tz, err := datetime.Timezone("")
	if err != nil {
		fmt.Printf("can't get timezone: %s\n", err.Error())
		os.Exit(1)
	}

	now := time.Now()

	from := datetime.DateParamToEpoch(*fromStr, tz, now, 0)
	if from == 0 && len(renderTargets) > 0 {
		fmt.Printf("invalid from: %s\n", *fromStr)
		os.Exit(1)
	}

	var until int64

	if *untilStr == "" && len(renderTargets) > 0 {
		*untilStr = "now"
	}

	until = datetime.DateParamToEpoch(*untilStr, tz, now, 0)
	if until == 0 && len(renderTargets) > 0 {
		fmt.Printf("invalid until: %s\n", *untilStr)
		os.Exit(1)
	}

	maxDataPoints, err := strconv.ParseInt(*maxDataPointsStr, 10, 64)
	if err != nil {
		fmt.Printf("invalid maxDataPoints: %s\n", *maxDataPointsStr)
		os.Exit(1)
	}

	httpClient := http.Client{
		Timeout: *timeout,
	}

	if *metricsFind != "" {
		formatFind := format
		if formatFind == client.FormatDefault {
			formatFind = client.FormatPb_v3
		}

		metricQuery, err := client.NewMetricsFind(*address, formatFind, *metricsFind, from, until)
		if err != nil {
			ec = 1

			fmt.Printf("0s %q = %q\n", metricQuery.QueryParams(), strings.TrimRight(err.Error(), "\n"))
		} else {
			r, duration, respHeader, err := metricQuery.Query(&httpClient)
			if respHeader != nil {
				fmt.Printf("Responce header: %+v\n", respHeader)
			}

			fmt.Printf("%v %q = ", duration, metricQuery.QueryParams())

			if err == nil {
				if len(r) > 0 {
					fmt.Println("[")

					for i, m := range r {
						fmt.Printf("  { Path: '%s', IsLeaf: %v }", m.Path, m.IsLeaf)

						if i < len(r)-1 {
							fmt.Println(",")
						} else {
							fmt.Println("")
						}
					}

					fmt.Println("]")
				} else {
					fmt.Println("[]")
				}
			} else {
				ec = 1

				fmt.Printf("%q\n", strings.TrimRight(err.Error(), "\n"))
			}
		}
	}

	if *tagsValues != "" {
		formatTags := format
		if formatTags == client.FormatDefault {
			formatTags = client.FormatJSON
		}

		tagsQuery, err := client.NewTagsValues(*address, formatTags, *tagsValues, *limit, from, until)
		if err != nil {
			ec = 1

			fmt.Printf("0s %q = %q\n", tagsQuery.QueryParams(), strings.TrimRight(err.Error(), "\n"))
		} else {
			r, duration, respHeader, err := tagsQuery.Query(&httpClient)
			if respHeader != nil {
				fmt.Printf("Responce header: %+v\n", respHeader)
			}

			fmt.Printf("%v %q = ", duration, tagsQuery.QueryParams())

			if err == nil {
				if len(r) > 0 {
					fmt.Println("[")

					for i, v := range r {
						fmt.Printf("  { Value: '%s' }", v)

						if i < len(r)-1 {
							fmt.Println(",")
						} else {
							fmt.Println("")
						}
					}

					fmt.Println("]")
				} else {
					fmt.Println("[]")
				}
			} else {
				ec = 1

				fmt.Printf("%q\n", strings.TrimRight(err.Error(), "\n"))
			}
		}
	}

	if *tagsNames != "" {
		formatTags := format
		if formatTags == client.FormatDefault {
			formatTags = client.FormatJSON
		}

		tagsQuery, err := client.NewTagsNames(*address, formatTags, *tagsNames, *limit, from, until)
		if err != nil {
			ec = 1

			fmt.Printf("0s %q = %q\n", tagsQuery.QueryParams(), strings.TrimRight(err.Error(), "\n"))
		} else {
			r, duration, respHeader, err := tagsQuery.Query(&httpClient)

			if respHeader != nil {
				fmt.Printf("Responce header: %+v\n", respHeader)
			}

			fmt.Printf("0s %q = \n", duration, tagsQuery.QueryParams())

			if err == nil {
				if len(r) > 0 {
					fmt.Println("[")

					for i, v := range r {
						fmt.Printf("  { Tag: '%s' }", v)

						if i < len(r)-1 {
							fmt.Println(",")
						} else {
							fmt.Println("")
						}
					}

					fmt.Println("]")
				} else {
					fmt.Println("[]")
				}
			} else {
				ec = 1

				fmt.Printf("%q\n", strings.TrimRight(err.Error(), "\n"))
			}
		}
	}

	if len(renderTargets) > 0 {
		formatRender := format
		if formatRender == client.FormatDefault {
			formatRender = client.FormatPb_v3
		}

		renderQuery, err := client.NewRender(*address, formatRender, renderTargets, []*carbonapi_v3_pb.FilteringFunction{}, maxDataPoints, from, until)
		if err != nil {
			ec = 1

			fmt.Printf("0s %q = %s\n", renderQuery.QueryParams(), strings.TrimRight(err.Error(), "\n"))
		} else {
			r, duration, respHeader, err := renderQuery.Query(&httpClient)

			if respHeader != nil {
				fmt.Printf("Responce header: %+v\n", respHeader)
			}

			fmt.Printf("%v %q = ", duration, renderQuery.QueryParams())

			if err == nil {
				if len(r) > 0 {
					fmt.Println("[")

					for i, m := range r {
						fmt.Println("  {")
						fmt.Printf("    Name: '%s', PathExpression: '%v',\n", m.Name, m.PathExpression)
						fmt.Printf("    ConsolidationFunc: %s, XFilesFactor: %f, AppliedFunctions: %s,\n", m.ConsolidationFunc, m.XFilesFactor, m.AppliedFunctions)
						fmt.Printf("    Start: %d, Stop: %d, Step: %d, RequestStart: %d, RequestStop: %d,\n", m.StartTime, m.StopTime, m.StepTime, m.RequestStartTime, m.RequestStopTime)
						fmt.Printf("    Values: %+v\n", m.Values)

						if i == len(r) {
							fmt.Println("  }")
						} else {
							fmt.Println("  },")
						}
					}

					fmt.Println("]")
				} else {
					fmt.Println("[]")
				}
			} else {
				ec = 1

				fmt.Printf("%q\n", strings.TrimRight(err.Error(), "\n"))
			}
		}
	}

	os.Exit(ec)
}
