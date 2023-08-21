package main

import (
	"encoding/json"
	"os"
	"time"
)

/**
 * This saves the `decypher`ed data into a JSON file
 **/

type FullFile struct {
	LastUpdated time.Time                 `json:"last_updated"`
	SourceFiles map[string]SourceFileData `json:"source_files"`
	YearIndex   map[int][]string          `json:"year_index"`
}

type SourceFileData struct {
	Year map[int]YearFileData `json:"year"`
}

type YearFileData struct {
	Month map[int]MonthFileData `json:"month"`
}

type MonthFileData struct {
	Category map[string]CategoryFileData `json:"category"`
	IpTotal  []IpTotalData               `json:"ip_total"`
}

type CategoryFileData struct {
	DateSummary    []SummarySet `json:"date_summary"`
	HourSummary    []SummarySet `json:"hour_summary"`
	WeekdaySummary []SummarySet `json:"weekday_summary"`
}

type IpTotalData struct {
	GeoIP GeoIP `json:"go_ip"`
	Hits  int
}

func Load() FullFile {
	var allMyData FullFile

	data, _ := os.ReadFile("./data_file.json")
	err := json.Unmarshal(data, &allMyData)
	if err != nil {
		panic(err)
	}

	return allMyData
}

func Save(allMyData FullFile) {
	// Create/ Update the YearIndex
	var year_index = make(map[int][]string)
	for source, years := range allMyData.SourceFiles {
		for year := range years.Year {
			year_index[year] = append(year_index[year], source)
		}
	}
	allMyData.YearIndex = year_index
	output, err := json.Marshal(allMyData)
	if err != nil {
		panic("That didn't work")
	}
	if os.WriteFile("./data_file.json", output, 0666) != nil {
		panic("This either")
	}

}

func Initialise() {
	var allMyData FullFile
	output, err := json.Marshal(allMyData)
	if err != nil {
		panic("That didn't work")
	}
	if os.WriteFile("./data_file.json", output, 0666) != nil {
		panic("This either")
	}
}
