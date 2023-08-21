package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInit(t *testing.T) {
	Initialise()
	assert.Equal(t, 1, monthTotal["CMTest01"][2021][1]["blog"].UniqueVisitors, "Unique Visitors did not match")
}
func TestLoad(t *testing.T) {
	Initialise()
	fullFile := Load()
	assert.Equal(t, 0, len(fullFile.SourceFiles), "Unique Visitors did not match")
}
func TestSave(t *testing.T) {
	var fullFile FullFile
	fullFile.SourceFiles = make(map[string]SourceFileData)
	fullFile.SourceFiles["ff"] = SourceFileData{Year: make(map[int]YearFileData)}
	fullFile.SourceFiles["ff"].Year[2021] = YearFileData{Month: make(map[int]MonthFileData)}
	fullFile.SourceFiles["ff"].Year[2021].Month[1] = MonthFileData{Category: make(map[string]CategoryFileData)}
	fullFile.SourceFiles["ff"].Year[2021].Month[1].Category["blog"] = CategoryFileData{
		DateSummary:    make([]SummarySet, 31),
		HourSummary:    make([]SummarySet, 24),
		WeekdaySummary: make([]SummarySet, 7),
	}
	Save(fullFile)
	fullFile = Load()
	assert.Equal(t, 1, len(fullFile.SourceFiles), "Sourcefiles length fail")
	assert.Equal(t, 1, len(fullFile.SourceFiles["ff"].Year), "Year length fail")
	assert.Equal(t, 1, len(fullFile.SourceFiles["ff"].Year[2021].Month), "Month length fail")
	assert.Equal(t, 1, len(fullFile.SourceFiles["ff"].Year[2021].Month[1].Category), "Category length fail")
}
