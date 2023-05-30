package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"log"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

type inputLine struct {
	ID    string
	Coast float64
	Time  time.Time
}

type candle struct {
	ID         string
	StartCoast float64
	EndCoast   float64
	MinCoast   float64
	MaxCoast   float64
	Time       time.Time
	Interval   time.Duration
}

func (c candle) ToCSV() []string {
	return []string{
		c.ID,
		fmt.Sprintf("%.2f", c.StartCoast),
		fmt.Sprintf("%.2f", c.MaxCoast),
		fmt.Sprintf("%.2f", c.MinCoast),
		fmt.Sprintf("%.2f", c.EndCoast),
		c.Time.Format(time.RFC3339),
		formatInterval(c.Interval),
	}
}

func main() {
	var (
		inputLines []inputLine
		scanner    = bufio.NewScanner(os.Stdin)
	)

	for scanner.Scan() {
		line := scanner.Text()

		if line == "" {
			break
		}

		lineParts := strings.Split(line, ",")
		if len(lineParts) < 3 {
			log.Fatalf("bad user input: %s", line)
		}

		coast, err := strconv.ParseFloat(lineParts[1], 64)
		if err != nil {
			log.Fatal(err)
		}

		t, err := time.Parse(time.RFC3339, lineParts[2])
		if err != nil {
			log.Fatal(err)
		}

		inputLines = append(inputLines, inputLine{
			ID:    lineParts[0],
			Coast: coast,
			Time:  t,
		})
	}

	candles := solution(inputLines)

	w := csv.NewWriter(os.Stdout)
	w.Comma = ','
	defer w.Flush()

	for _, candle := range candles {
		if err := w.Write(candle.ToCSV()); err != nil {
			log.Fatal(err)
		}
	}
}

func solution(inputLines []inputLine) []candle {
	idLinesMap := make(map[string][]inputLine)

	for _, line := range inputLines {
		idLinesMap[line.ID] = append(idLinesMap[line.ID], line)
	}

	idCandlesMap := make(map[string][]candle)

	for id, lines := range idLinesMap {
		times := make([]time.Time, len(lines))

		for i := 0; i < len(lines); i++ {
			times[i] = lines[i].Time
		}

		intervals := makeIntervals(times)

		for i := 0; i < len(intervals); i++ {
			dur := intervals[i]
			timeSet := make(map[time.Time]struct{})

			for _, t := range times {
				startTime := t.Truncate(dur)
				endTime := startTime.Add(dur)

				if _, ok := timeSet[startTime]; ok {
					continue
				}

				timeSet[startTime] = struct{}{}

				idCandlesMap[id] = append(idCandlesMap[id], candle{
					ID:         id,
					StartCoast: startCoastOnInterval(startTime, endTime, lines),
					EndCoast:   endCoastOnInterval(startTime, endTime, lines),
					MinCoast:   minOnInterval(startTime, endTime, lines),
					MaxCoast:   maxOnInterval(startTime, endTime, lines),
					Time:       startTime,
					Interval:   dur,
				})
			}
		}
	}

	var result []candle

	for _, candles := range idCandlesMap {
		result = append(result, candles...)
	}

	sort.Slice(result, func(i, j int) bool {
		if result[i].ID != result[j].ID {
			return result[i].ID < result[j].ID
		}
		if result[i].Interval != result[j].Interval {
			return result[i].Interval < result[j].Interval
		}
		return result[i].Time.Before(result[j].Time)
	})

	return result
}

func makeIntervals(times []time.Time) []time.Duration {
	durTimeSet := make(map[time.Duration]map[time.Time]struct{})

	for _, dur := range []time.Duration{time.Minute, 2 * time.Minute, 5 * time.Minute} {
		for i := 0; i < len(times)-1; i++ {
			t2 := times[i+1].Truncate(dur)
			t1 := times[i].Truncate(dur)
			curDur := t2.Sub(t1)

			if curDur == 0 {
				curDur = dur
			}

			if durTimeSet[curDur] == nil {
				durTimeSet[curDur] = make(map[time.Time]struct{})
			}

			durTimeSet[curDur][t1] = struct{}{}
			durTimeSet[curDur][t2] = struct{}{}
		}
	}

	result := make([]time.Duration, 0, len(durTimeSet))

	for dur, times := range durTimeSet {
		if len(times) < 2 {
			continue
		}

		result = append(result, dur)
	}

	return result
}

func minOnInterval(startTime, endTime time.Time, lines []inputLine) float64 {
	min := math.MaxFloat64

	for i := 0; i < len(lines); i++ {
		curTime := lines[i].Time.Unix()

		if startTime.Unix() <= curTime && curTime < endTime.Unix() {
			if lines[i].Coast < min {
				min = lines[i].Coast
			}
		}
	}

	return min
}

func maxOnInterval(startTime, endTime time.Time, lines []inputLine) float64 {
	max := -1.0

	for i := 0; i < len(lines); i++ {
		curTime := lines[i].Time.Unix()

		if startTime.Unix() <= curTime && curTime < endTime.Unix() {
			if lines[i].Coast > max {
				max = lines[i].Coast
			}
		}
	}

	return max
}

func startCoastOnInterval(startTime, endTime time.Time, lines []inputLine) float64 {
	for i := 0; i < len(lines); i++ {
		curTime := lines[i].Time.Unix()

		if startTime.Unix() <= curTime && curTime < endTime.Unix() {
			return lines[i].Coast
		}
	}

	return -1.0
}

func endCoastOnInterval(startTime, endTime time.Time, lines []inputLine) float64 {
	for i := len(lines) - 1; i >= 0; i-- {
		curTime := lines[i].Time.Unix()

		if startTime.Unix() <= curTime && curTime < endTime.Unix() {
			return lines[i].Coast
		}
	}

	return -1.0
}

func formatInterval(interval time.Duration) string {
	result := interval.String()
	idx := strings.Index(result, "m")

	if idx == -1 {
		return result
	}

	if idx == len(result)-1 {
		return result
	}

	return result[:idx+1]
}
