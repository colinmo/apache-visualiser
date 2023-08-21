package main

import (
	"database/sql"
	"fmt"
	"log"
	"net"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	//_ "github.com/mattn/go-sqlite3"
	_ "github.com/go-sql-driver/mysql"
	"github.com/oschwald/geoip2-golang"
	"github.com/pattfy/useragent"
)

/*
 * Receives an ApacheEntry and updates a database of summaries, lookups, translates etc.
 */

/*
 ISSUES:
	* Since this is going across files, I don't know how to reliably work out visits yet
*/

type StorageEntry struct {
	SourceId           string
	LineNumber         int
	IPAddress          string
	Identd             string
	User               string
	RequestDateAndTime time.Time
	RequestMethod      string
	RequestResource    string
	RequestProtocol    string
	StatusCode         int
	BytesSent          string
	Referrer           string
	UserAgent          string
	Country            string
	CountryCode        string
	Category           string
	Filetype           string
	OperatingSystem    string
	Browser            string
	IsBot              bool
	IsSearchengine     bool
	IsMobile           bool
	IsTablet           bool
	MonthBit           int
	YearBit            int
}

type SummarySet struct {
	SourceId       string
	UniqueVisitors int
	Visits         int
	KBytes         int64
	Pages          int
	Hit            int
	NonBotPages    int
	NonBotHits     int
}
type MonthlySummary struct {
	Month    int
	Year     int
	Category string
	Summary  SummarySet
}
type DateSummary struct {
	Date     int
	Month    int
	Year     int
	Category string
	Summary  SummarySet
}
type WeekdaySummary struct {
	Weekday  int
	Month    int
	Year     int
	Category string
	Summary  SummarySet
}
type HourSummary struct {
	Hour     int
	Month    int
	Year     int
	Category string
	Summary  SummarySet
}
type URLTotal struct {
	URL    string
	Totals []MonthlySummary
}
type CountryTotal struct {
	Country string
	Totals  []MonthlySummary
}
type IPTotal struct {
	IP     string
	Totals []MonthlySummary
}
type ExtensionTotal struct {
	Extension string
	Totals    []MonthlySummary
}
type OSTotal struct {
	OS     string
	Totals []MonthlySummary
}
type BrowserTotal struct {
	Browser string
	Totals  []MonthlySummary
}
type CodeTotal struct {
	Code   int
	Totals []MonthlySummary
}
type ActionTotal struct {
	Action string
	Totals []MonthlySummary
}
type URLList struct {
	Month int
	Year  int
	URLs  []string
}
type ActionList struct {
	Month  int
	Year   int
	Action []string
}
type GeoIP struct {
	IP          string
	CountryCode string `json:"countrycode"`
	CountryName string `json:"name"`
}

var dbOfLogs *sql.DB
var batchSize int
var geoDB *geoip2.Reader

func ConnectToDB() {
	batchSize = 2000
	// file:test.s3db?_auth&_auth_user=admin&_auth_pass=admin&_auth_crypt=sha1
	var err error
	// dbOfLogs, err = sql.Open("sqlite3", "./foo.db")
	dbOfLogs, err = sql.Open("mysql", "apache:apache@tcp(127.0.0.1:3306)/apache")
	if err != nil {
		log.Fatal(err)
	}

	unsavedRows = make([]StorageEntry, 0, batchSize)

	geoDB, _ = geoip2.Open("./GeoLite2-Country.mmdb")
}

func ReleaseDB() {
	if dbOfLogs != nil {
		dbOfLogs.Close()
	}
}

func ClearExistingSummaries(source string) {
	monthTotal = make(map[string]map[int]map[int]map[string]*SummarySet)
	dateTotal = make(map[string]map[int]map[int]map[int]map[string]*SummarySet)
	weekdayTotal = make(map[string]map[int]map[int]map[int]map[string]*SummarySet)
	hourTotal = make(map[string]map[int]map[int]map[int]map[string]*SummarySet)

	monthTotal[source] = make(map[int]map[int]map[string]*SummarySet)
	dateTotal[source] = make(map[int]map[int]map[int]map[string]*SummarySet)
	weekdayTotal[source] = make(map[int]map[int]map[int]map[string]*SummarySet)
	hourTotal[source] = make(map[int]map[int]map[int]map[string]*SummarySet)
}
func ClearExistingForSource(source string) error {
	ClearExistingSummaries(source)

	// _, err := dbOfLogs.Exec("DELETE FROM apache_entry WHERE sourceid = ?", source)
	_, err := dbOfLogs.Exec("DELETE FROM month_total WHERE sourceid = ?", source)
	if err != nil {
		return err
	}
	_, err = dbOfLogs.Exec("DELETE FROM weekday_total WHERE sourceid = ?", source)
	if err != nil {
		return err
	}
	_, err = dbOfLogs.Exec("DELETE FROM hour_total WHERE sourceid = ?", source)
	if err != nil {
		return err
	}
	_, err = dbOfLogs.Exec("DELETE FROM date_total WHERE sourceid = ?", source)
	if err != nil {
		return err
	}
	_, err = dbOfLogs.Exec("DELETE FROM ip_total WHERE sourceid = ?", source)
	return err
}

func ProcessEntry(entry ApacheEntry) {
	// Process user agent field
	userAgent := useragent.New(entry.UserAgent)
	// Process file type by Extension
	filetype := filepath.Ext(entry.RequestResource)
	mep := strings.Split(filetype, "?")
	if len(mep) > 1 {
		filetype = mep[0]
	}
	if len(filetype) > 20 {
		filetype = filetype[0:20]
	}
	// Process bytes sent
	if entry.BytesSent == "-" {
		entry.BytesSent = "0"
	}
	// Process country
	foundCountry := "Unknown"
	foundCC := "??"
	parsedIP := net.ParseIP(entry.IPAddress)
	if parsedIP != nil {
		country, err := geoDB.Country(parsedIP)
		if err == nil {
			foundCountry = country.Country.Names["en"]
			foundCC = country.Country.IsoCode
		}
	}
	// Process category
	category := "basic"
	if len(entry.RequestResource) > 5 && entry.RequestResource[0:6] == "/theme" {
		category = "theme"
	} else if (len(entry.RequestResource) > 18 && entry.RequestResource[0:19] == "/journal/wp-uploads") ||
		(len(entry.RequestResource) > 10 && entry.RequestResource[0:11] == "/blog/media") {
		category = "media"
	} else if (len(entry.RequestResource) > 7 && entry.RequestResource[0:8] == "/journal") ||
		(len(entry.RequestResource) > 4 && entry.RequestResource[0:5] == "/blog") {
		category = "blog"
	} else if len(entry.RequestResource) > 4 && entry.RequestResource[0:5] == "/code" {
		category = "code"
	}

	storageEntry := StorageEntry{
		SourceId:           entry.SourceId,
		LineNumber:         entry.LineNumber,
		IPAddress:          entry.IPAddress,
		Identd:             entry.Identd,
		User:               entry.User,
		RequestDateAndTime: entry.RequestDateAndTime,
		RequestMethod:      entry.RequestMethod,
		RequestResource:    entry.RequestResource,
		RequestProtocol:    entry.RequestProtocol,
		StatusCode:         entry.StatusCode,
		BytesSent:          entry.BytesSent,
		Referrer:           entry.Referrer,
		UserAgent:          entry.UserAgent,
		Filetype:           filetype,
		Country:            foundCountry,
		CountryCode:        foundCC,
		Category:           category,
		OperatingSystem:    userAgent.Platform.Name(),
		Browser:            userAgent.Browser.Name(),
		IsBot:              userAgent.IsBot(),
		IsSearchengine:     userAgent.Bot.IsSearchEngine(),
		IsMobile:           userAgent.Device.IsMobile(),
		IsTablet:           userAgent.Device.IsTablet(),
	}
	SaveEntry(storageEntry)
}

type IPEntry struct {
	CountryCode string
	EntryCount  int
}

// Individual lines
var unsavedRows []StorageEntry

// Aggregate summaries
var monthTotal map[string]map[int]map[int]map[string]*SummarySet           // SourceId, Year, Month, Category
var dateTotal map[string]map[int]map[int]map[int]map[string]*SummarySet    // SourceId, Year, Month, Date, Category
var weekdayTotal map[string]map[int]map[int]map[int]map[string]*SummarySet // SourceId, Year, Month, Weekday, Category
var hourTotal map[string]map[int]map[int]map[int]map[string]*SummarySet    // SourceId, Year, Month, Hour, Category
var ipTotal map[string]map[int]map[int]map[string]*IPEntry                 // SourceId, Year, Month, IP => Count

var hitTotal map[string]map[int]map[int]map[string]SummarySet       // SourceId, Year, Month, Category, ?
var pageTotal map[string]map[int]map[int]map[string]SummarySet      // SourceId, Year, Month, Category, Page
var robotTotal map[string]map[int]map[int]map[string]SummarySet     // SourceId, Year, Month, Category, RobotName
var humanTotal map[string]map[int]map[int]map[string]SummarySet     // SourceId, Year, Month, Category, BrowserName
var extensionTotal map[string]map[int]map[int]map[string]SummarySet // SourceId, Year, Month, Category, Extension
var osTotal map[string]map[int]map[int]map[string]SummarySet        // SourceId, Year, Month, Category, OS
var browserTotal map[string]map[int]map[int]map[string]SummarySet   // SourceId, Year, Month, Category, BrowserName
var referrerTotal map[string]map[int]map[int]map[string]SummarySet  // SourceId, Year, Month, Category, Referrer
var codeTotal map[string]map[int]map[int]map[string]SummarySet      // SourceId, Year, Month, Category, ResponseCode
var fourohfourList map[string]map[int]map[int]map[string]int        // SourceId, Year, Month, Category, Page (404 code specifically)
var actionList map[string]map[int]map[int]map[string]int            // SourceId, Year, Month, Category, Action String

// IP Addresses seen in each period
var monthSeenVisitors map[string]map[int]map[int]map[string][]string
var dateSeenVisitors map[string]map[int]map[int]map[int]map[string][]string
var weekdaySeenVisitors map[string]map[int]map[int]map[int]map[string][]string
var hourSeenVisitors map[string]map[int]map[int]map[int]map[string][]string

var initialCategoryValues map[string]*SummarySet

func SaveEntry(dbEntry StorageEntry) {
	// Individual line
	unsavedRows = append(unsavedRows, dbEntry)

	// Update aggregates
	yearBit, monthBitS, dateBit := dbEntry.RequestDateAndTime.Date()
	monthBit := int(monthBitS)
	weekBit := int(dbEntry.RequestDateAndTime.Weekday())
	hourBit := int(dbEntry.RequestDateAndTime.Hour())
	category := dbEntry.Category
	if category == "" {
		category = "basic"
	}

	// SUMMARIES //
	if monthTotal[dbEntry.SourceId] == nil {
		monthTotal = make(map[string]map[int]map[int]map[string]*SummarySet)
		dateTotal = make(map[string]map[int]map[int]map[int]map[string]*SummarySet)
		weekdayTotal = make(map[string]map[int]map[int]map[int]map[string]*SummarySet)
		hourTotal = make(map[string]map[int]map[int]map[int]map[string]*SummarySet)
		monthTotal[dbEntry.SourceId] = make(map[int]map[int]map[string]*SummarySet)
		dateTotal[dbEntry.SourceId] = make(map[int]map[int]map[int]map[string]*SummarySet)
		weekdayTotal[dbEntry.SourceId] = make(map[int]map[int]map[int]map[string]*SummarySet)
		hourTotal[dbEntry.SourceId] = make(map[int]map[int]map[int]map[string]*SummarySet)
	}
	if monthTotal[dbEntry.SourceId][yearBit] == nil {
		monthTotal[dbEntry.SourceId][yearBit] = make(map[int]map[string]*SummarySet)
		dateTotal[dbEntry.SourceId][yearBit] = make(map[int]map[int]map[string]*SummarySet)
		weekdayTotal[dbEntry.SourceId][yearBit] = make(map[int]map[int]map[string]*SummarySet)
		hourTotal[dbEntry.SourceId][yearBit] = make(map[int]map[int]map[string]*SummarySet)
	}
	if monthTotal[dbEntry.SourceId][yearBit][monthBit] == nil {
		monthTotal[dbEntry.SourceId][yearBit][monthBit] = make(map[string]*SummarySet)
		monthTotal[dbEntry.SourceId][yearBit][monthBit]["blog"] = &SummarySet{dbEntry.SourceId, 0, 0, 0, 0, 0, 0, 0}
		monthTotal[dbEntry.SourceId][yearBit][monthBit]["media"] = &SummarySet{dbEntry.SourceId, 0, 0, 0, 0, 0, 0, 0}
		monthTotal[dbEntry.SourceId][yearBit][monthBit]["code"] = &SummarySet{dbEntry.SourceId, 0, 0, 0, 0, 0, 0, 0}
		monthTotal[dbEntry.SourceId][yearBit][monthBit]["theme"] = &SummarySet{dbEntry.SourceId, 0, 0, 0, 0, 0, 0, 0}
		monthTotal[dbEntry.SourceId][yearBit][monthBit]["basic"] = &SummarySet{dbEntry.SourceId, 0, 0, 0, 0, 0, 0, 0}

		dateTotal[dbEntry.SourceId][yearBit][monthBit] = make(map[int]map[string]*SummarySet)
		for t := 1; t < time.Date(yearBit, time.Month(monthBit+1), 0, 0, 0, 0, 0, time.Local).Day()+1; t++ {
			dateTotal[dbEntry.SourceId][yearBit][monthBit][t] = make(map[string]*SummarySet)
			dateTotal[dbEntry.SourceId][yearBit][monthBit][t]["blog"] = &SummarySet{dbEntry.SourceId, 0, 0, 0, 0, 0, 0, 0}
			dateTotal[dbEntry.SourceId][yearBit][monthBit][t]["media"] = &SummarySet{dbEntry.SourceId, 0, 0, 0, 0, 0, 0, 0}
			dateTotal[dbEntry.SourceId][yearBit][monthBit][t]["code"] = &SummarySet{dbEntry.SourceId, 0, 0, 0, 0, 0, 0, 0}
			dateTotal[dbEntry.SourceId][yearBit][monthBit][t]["theme"] = &SummarySet{dbEntry.SourceId, 0, 0, 0, 0, 0, 0, 0}
			dateTotal[dbEntry.SourceId][yearBit][monthBit][t]["basic"] = &SummarySet{dbEntry.SourceId, 0, 0, 0, 0, 0, 0, 0}
		}
		weekdayTotal[dbEntry.SourceId][yearBit][monthBit] = make(map[int]map[string]*SummarySet)
		for t := 0; t < 7; t++ {
			weekdayTotal[dbEntry.SourceId][yearBit][monthBit][t] = make(map[string]*SummarySet)
			weekdayTotal[dbEntry.SourceId][yearBit][monthBit][t]["blog"] = &SummarySet{dbEntry.SourceId, 0, 0, 0, 0, 0, 0, 0}
			weekdayTotal[dbEntry.SourceId][yearBit][monthBit][t]["media"] = &SummarySet{dbEntry.SourceId, 0, 0, 0, 0, 0, 0, 0}
			weekdayTotal[dbEntry.SourceId][yearBit][monthBit][t]["code"] = &SummarySet{dbEntry.SourceId, 0, 0, 0, 0, 0, 0, 0}
			weekdayTotal[dbEntry.SourceId][yearBit][monthBit][t]["theme"] = &SummarySet{dbEntry.SourceId, 0, 0, 0, 0, 0, 0, 0}
			weekdayTotal[dbEntry.SourceId][yearBit][monthBit][t]["basic"] = &SummarySet{dbEntry.SourceId, 0, 0, 0, 0, 0, 0, 0}
		}
		hourTotal[dbEntry.SourceId][yearBit][monthBit] = make(map[int]map[string]*SummarySet)
		for t := 0; t < 24; t++ {
			hourTotal[dbEntry.SourceId][yearBit][monthBit][t] = make(map[string]*SummarySet)
			hourTotal[dbEntry.SourceId][yearBit][monthBit][t]["blog"] = &SummarySet{dbEntry.SourceId, 0, 0, 0, 0, 0, 0, 0}
			hourTotal[dbEntry.SourceId][yearBit][monthBit][t]["media"] = &SummarySet{dbEntry.SourceId, 0, 0, 0, 0, 0, 0, 0}
			hourTotal[dbEntry.SourceId][yearBit][monthBit][t]["code"] = &SummarySet{dbEntry.SourceId, 0, 0, 0, 0, 0, 0, 0}
			hourTotal[dbEntry.SourceId][yearBit][monthBit][t]["theme"] = &SummarySet{dbEntry.SourceId, 0, 0, 0, 0, 0, 0, 0}
			hourTotal[dbEntry.SourceId][yearBit][monthBit][t]["basic"] = &SummarySet{dbEntry.SourceId, 0, 0, 0, 0, 0, 0, 0}
		}
	}
	existingMonthSummary := monthTotal[dbEntry.SourceId][yearBit][monthBit]
	existingDateSummary := dateTotal[dbEntry.SourceId][yearBit][monthBit][dateBit]
	existingWeekdaySummary := weekdayTotal[dbEntry.SourceId][yearBit][monthBit][weekBit]
	existingHourSummary := hourTotal[dbEntry.SourceId][yearBit][monthBit][hourBit]

	// Unique Visitors
	if monthSeenVisitors[dbEntry.SourceId] == nil {
		monthSeenVisitors = make(map[string]map[int]map[int]map[string][]string)
		monthSeenVisitors[dbEntry.SourceId] = make(map[int]map[int]map[string][]string)
		dateSeenVisitors = make(map[string]map[int]map[int]map[int]map[string][]string)
		dateSeenVisitors[dbEntry.SourceId] = make(map[int]map[int]map[int]map[string][]string)
		weekdaySeenVisitors = make(map[string]map[int]map[int]map[int]map[string][]string)
		weekdaySeenVisitors[dbEntry.SourceId] = make(map[int]map[int]map[int]map[string][]string)
		hourSeenVisitors = make(map[string]map[int]map[int]map[int]map[string][]string)
		hourSeenVisitors[dbEntry.SourceId] = make(map[int]map[int]map[int]map[string][]string)
		ipTotal = make(map[string]map[int]map[int]map[string]*IPEntry)
		ipTotal[dbEntry.SourceId] = make(map[int]map[int]map[string]*IPEntry)
	}
	if monthSeenVisitors[dbEntry.SourceId][yearBit] == nil {
		monthSeenVisitors[dbEntry.SourceId][yearBit] = make(map[int]map[string][]string)
		dateSeenVisitors[dbEntry.SourceId][yearBit] = make(map[int]map[int]map[string][]string)
		weekdaySeenVisitors[dbEntry.SourceId][yearBit] = make(map[int]map[int]map[string][]string)
		hourSeenVisitors[dbEntry.SourceId][yearBit] = make(map[int]map[int]map[string][]string)
		ipTotal[dbEntry.SourceId][yearBit] = make(map[int]map[string]*IPEntry)
	}
	if monthSeenVisitors[dbEntry.SourceId][yearBit][monthBit] == nil {
		// Lets build the basics for all summaries
		monthSeenVisitors[dbEntry.SourceId][yearBit][monthBit] = make(map[string][]string)
		monthSeenVisitors[dbEntry.SourceId][yearBit][monthBit]["blog"] = []string{}
		monthSeenVisitors[dbEntry.SourceId][yearBit][monthBit]["media"] = []string{}
		monthSeenVisitors[dbEntry.SourceId][yearBit][monthBit]["code"] = []string{}
		monthSeenVisitors[dbEntry.SourceId][yearBit][monthBit]["theme"] = []string{}
		monthSeenVisitors[dbEntry.SourceId][yearBit][monthBit]["basic"] = []string{}
		ipTotal[dbEntry.SourceId][yearBit][monthBit] = map[string]*IPEntry{}

		dateSeenVisitors[dbEntry.SourceId][yearBit][monthBit] = map[int]map[string][]string{}
		for t := 1; t < time.Date(yearBit, time.Month(monthBit+1), 0, 0, 0, 0, 0, time.Local).Day()+1; t++ {
			dateSeenVisitors[dbEntry.SourceId][yearBit][monthBit][t] = make(map[string][]string)
			dateSeenVisitors[dbEntry.SourceId][yearBit][monthBit][t]["blog"] = []string{}
			dateSeenVisitors[dbEntry.SourceId][yearBit][monthBit][t]["media"] = []string{}
			dateSeenVisitors[dbEntry.SourceId][yearBit][monthBit][t]["code"] = []string{}
			dateSeenVisitors[dbEntry.SourceId][yearBit][monthBit][t]["theme"] = []string{}
			dateSeenVisitors[dbEntry.SourceId][yearBit][monthBit][t]["basic"] = []string{}
		}
		weekdaySeenVisitors[dbEntry.SourceId][yearBit][monthBit] = map[int]map[string][]string{}
		for t := 0; t < 7; t++ {
			weekdaySeenVisitors[dbEntry.SourceId][yearBit][monthBit][t] = make(map[string][]string)
			weekdaySeenVisitors[dbEntry.SourceId][yearBit][monthBit][t]["blog"] = []string{}
			weekdaySeenVisitors[dbEntry.SourceId][yearBit][monthBit][t]["media"] = []string{}
			weekdaySeenVisitors[dbEntry.SourceId][yearBit][monthBit][t]["code"] = []string{}
			weekdaySeenVisitors[dbEntry.SourceId][yearBit][monthBit][t]["theme"] = []string{}
			weekdaySeenVisitors[dbEntry.SourceId][yearBit][monthBit][t]["basic"] = []string{}
		}
		hourSeenVisitors[dbEntry.SourceId][yearBit][monthBit] = map[int]map[string][]string{}
		for t := 0; t < 24; t++ {
			hourSeenVisitors[dbEntry.SourceId][yearBit][monthBit][t] = make(map[string][]string)
			hourSeenVisitors[dbEntry.SourceId][yearBit][monthBit][t]["blog"] = []string{}
			hourSeenVisitors[dbEntry.SourceId][yearBit][monthBit][t]["media"] = []string{}
			hourSeenVisitors[dbEntry.SourceId][yearBit][monthBit][t]["code"] = []string{}
			hourSeenVisitors[dbEntry.SourceId][yearBit][monthBit][t]["theme"] = []string{}
			hourSeenVisitors[dbEntry.SourceId][yearBit][monthBit][t]["basic"] = []string{}
		}
	}

	var found bool
	//Month unique
	found = false
	for _, v := range monthSeenVisitors[dbEntry.SourceId][yearBit][monthBit][category] {
		if dbEntry.IPAddress == v {
			found = true
			// record another IP hit
			ipTotal[dbEntry.SourceId][yearBit][monthBit][dbEntry.IPAddress].EntryCount++
			break
		}
	}
	if !found {
		monthTotal[dbEntry.SourceId][yearBit][monthBit][category].UniqueVisitors++
		monthSeenVisitors[dbEntry.SourceId][yearBit][monthBit][category] =
			append(monthSeenVisitors[dbEntry.SourceId][yearBit][monthBit][category],
				dbEntry.IPAddress)
		ipTotal[dbEntry.SourceId][yearBit][monthBit][dbEntry.IPAddress] = &IPEntry{EntryCount: 1, CountryCode: dbEntry.CountryCode}
	}
	// Date unique
	found = false
	for _, v := range dateSeenVisitors[dbEntry.SourceId][yearBit][monthBit][dateBit][category] {
		if dbEntry.IPAddress == v {
			found = true
			break
		}
	}
	if !found {
		dateTotal[dbEntry.SourceId][yearBit][monthBit][dateBit][category].UniqueVisitors++
		dateSeenVisitors[dbEntry.SourceId][yearBit][monthBit][dateBit][category] =
			append(dateSeenVisitors[dbEntry.SourceId][yearBit][monthBit][dateBit][category],
				dbEntry.IPAddress)
	}
	// Weekday Unique
	found = false
	for _, v := range weekdaySeenVisitors[dbEntry.SourceId][yearBit][monthBit][weekBit][category] {
		if dbEntry.IPAddress == v {
			found = true
			break
		}
	}
	if !found {
		weekdayTotal[dbEntry.SourceId][yearBit][monthBit][weekBit][category].UniqueVisitors++
		weekdaySeenVisitors[dbEntry.SourceId][yearBit][monthBit][weekBit][category] =
			append(weekdaySeenVisitors[dbEntry.SourceId][yearBit][monthBit][weekBit][category],
				dbEntry.IPAddress)
	}
	// Hour Unique
	found = false
	for _, v := range hourSeenVisitors[dbEntry.SourceId][yearBit][monthBit][hourBit][category] {
		if dbEntry.IPAddress == v {
			found = true
			break
		}
	}
	if !found {
		hourTotal[dbEntry.SourceId][yearBit][monthBit][hourBit][category].UniqueVisitors++
		hourSeenVisitors[dbEntry.SourceId][yearBit][monthBit][hourBit][category] =
			append(hourSeenVisitors[dbEntry.SourceId][yearBit][monthBit][hourBit][category],
				dbEntry.IPAddress)
	}

	// Page Counts
	// pageCount := existingMonthSummary.Pages
	// nonBotPages := existingMonthSummary.NonBotPages
	// nonBotHits := existingMonthSummary.NonBotHits
	monthPageCount := existingMonthSummary[category].Pages
	monthNonBotPages := existingMonthSummary[category].NonBotPages
	monthNonBotHits := existingMonthSummary[category].NonBotHits
	datePageCount := existingDateSummary[category].Pages
	dateNonBotPages := existingDateSummary[category].NonBotPages
	dateNonBotHits := existingDateSummary[category].NonBotHits
	weekdayPageCount := existingWeekdaySummary[category].Pages
	weekdayNonBotPages := existingWeekdaySummary[category].NonBotPages
	weekdayNonBotHits := existingWeekdaySummary[category].NonBotHits
	hourPageCount := existingHourSummary[category].Pages
	hourNonBotPages := existingHourSummary[category].NonBotPages
	hourNonBotHits := existingHourSummary[category].NonBotHits
	// File types
	for _, v := range []string{0: ".html", 1: "", 2: ".css", 3: ".js", 4: ".php"} {
		if dbEntry.Filetype == v {
			monthPageCount++
			datePageCount++
			weekdayPageCount++
			hourPageCount++
			if !dbEntry.IsBot {
				monthNonBotPages++
				dateNonBotPages++
				weekdayNonBotPages++
				hourNonBotPages++
			}
		}
	}
	if !dbEntry.IsBot {
		monthNonBotHits++
		dateNonBotHits++
		weekdayNonBotHits++
		hourNonBotHits++
	}
	bytesSentInt, _ := strconv.ParseInt(dbEntry.BytesSent, 10, 64)
	// Totals
	monthTotal[dbEntry.SourceId][yearBit][monthBit][category] = &SummarySet{
		SourceId:       dbEntry.SourceId,
		UniqueVisitors: monthTotal[dbEntry.SourceId][yearBit][monthBit][category].UniqueVisitors,
		Visits:         0,
		KBytes:         monthTotal[dbEntry.SourceId][yearBit][monthBit][category].KBytes + bytesSentInt,
		Pages:          monthPageCount,
		Hit:            monthTotal[dbEntry.SourceId][yearBit][monthBit][category].Hit + 1,
		NonBotPages:    monthNonBotPages,
		NonBotHits:     monthNonBotHits,
	}
	dateTotal[dbEntry.SourceId][yearBit][monthBit][dateBit][category] = &SummarySet{
		SourceId:       dbEntry.SourceId,
		UniqueVisitors: dateTotal[dbEntry.SourceId][yearBit][monthBit][dateBit][category].UniqueVisitors,
		KBytes:         dateTotal[dbEntry.SourceId][yearBit][monthBit][dateBit][category].KBytes + bytesSentInt,
		Pages:          datePageCount,
		Hit:            dateTotal[dbEntry.SourceId][yearBit][monthBit][dateBit][category].Hit + 1,
		NonBotPages:    dateNonBotPages,
		NonBotHits:     dateNonBotHits,
	}
	weekdayTotal[dbEntry.SourceId][yearBit][monthBit][weekBit][category] = &SummarySet{
		SourceId:       dbEntry.SourceId,
		UniqueVisitors: weekdayTotal[dbEntry.SourceId][yearBit][monthBit][weekBit][category].UniqueVisitors,
		KBytes:         weekdayTotal[dbEntry.SourceId][yearBit][monthBit][weekBit][category].KBytes + bytesSentInt,
		Pages:          weekdayPageCount,
		Hit:            weekdayTotal[dbEntry.SourceId][yearBit][monthBit][weekBit][category].Hit + 1,
		NonBotPages:    weekdayNonBotPages,
		NonBotHits:     weekdayNonBotHits,
	}
	hourTotal[dbEntry.SourceId][yearBit][monthBit][hourBit][category] = &SummarySet{
		SourceId:       dbEntry.SourceId,
		UniqueVisitors: hourTotal[dbEntry.SourceId][yearBit][monthBit][hourBit][category].UniqueVisitors,
		KBytes:         hourTotal[dbEntry.SourceId][yearBit][monthBit][hourBit][category].KBytes + bytesSentInt,
		Pages:          hourPageCount,
		Hit:            hourTotal[dbEntry.SourceId][yearBit][monthBit][hourBit][category].Hit + 1,
		NonBotPages:    hourNonBotPages,
		NonBotHits:     hourNonBotHits,
	}
}

func SaveToDb() {
	// Monthly Summaries
	var stmt string
	for sourceid, group1 := range monthTotal {
		for yearbit, group2 := range group1 {
			for monthbit, group3 := range group2 {
				// IP Address
				valueStrings := make([]string, 0, len(unsavedRows))
				valueArgs := make([]interface{}, 0, len(ipTotal[sourceid][yearbit][monthbit])*2)
				ipCounter := 0
				for ip, count := range ipTotal[sourceid][yearbit][monthbit] {
					valueStrings = append(valueStrings, "(?, ?, ?, ?, ?, ?)")
					valueArgs = append(valueArgs, sourceid, yearbit, monthbit, ip, count.EntryCount, count.CountryCode)
					ipCounter++
					if ipCounter%1000 == 0 {
						stmt = fmt.Sprintf(
							`INSERT INTO ip_total
						(sourceid, year, month, ip, hits, countrycode)
						VALUES %s`,
							strings.Join(valueStrings, ","))
						_, err := dbOfLogs.Exec(stmt, valueArgs...)
						if err != nil {
							fmt.Printf("Failed\n")
							log.Fatal(err)
						}
						valueStrings = make([]string, 0, len(unsavedRows))
						valueArgs = make([]interface{}, 0, len(ipTotal[sourceid][yearbit][monthbit])*2)
					}
				}
				if len(valueArgs) > 0 {
					stmt = fmt.Sprintf(
						`INSERT INTO ip_total
				(sourceid, year, month, ip, hits, countrycode)
				VALUES %s`,
						strings.Join(valueStrings, ","))
					_, err := dbOfLogs.Exec(stmt, valueArgs...)
					if err != nil {
						fmt.Printf("Failed\n")
						log.Fatal(err)
					}
				}

				for category, summary := range group3 {
					// Month Totals
					_, err := dbOfLogs.Exec(`
					INSERT INTO month_total
					(sourceid, month, year, category, uniquevisitors, visits,
					kbytes, pages, hit, notbotpage, notbothit) values 
					(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
						sourceid, monthbit, yearbit, category, summary.UniqueVisitors, summary.Visits,
						summary.KBytes, summary.Pages, summary.Hit, summary.NonBotPages, summary.NonBotHits,
					)
					if err != nil {
						fmt.Printf("Failed\n")
						log.Fatal(err)
					}

					// HOUR TOTALS
					valueStrings = make([]string, 0, len(hourTotal))
					valueArgs = make([]interface{}, 0, len(hourTotal)*2)
					for hour, summary := range hourTotal[sourceid][yearbit][monthbit] {
						valueStrings = append(valueStrings, "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
						valueArgs = append(valueArgs, sourceid, yearbit, monthbit, hour, category,
							summary[category].UniqueVisitors, summary[category].Visits, summary[category].KBytes,
							summary[category].Pages, summary[category].Hit, summary[category].NonBotPages, summary[category].NonBotHits)
					}
					stmt = fmt.Sprintf(
						`INSERT INTO hour_total ( sourceid, year, month, hour, category,
							uniquevisitors, visits, kbytes, pages, hit, notbotpage,
							notbothit) values %s`,
						strings.Join(valueStrings, ","))
					_, err = dbOfLogs.Exec(stmt, valueArgs...)
					if err != nil {
						fmt.Printf("Failed\n")
						log.Fatal(err)
					}
					// WEEKDAY TOTALS
					valueStrings = make([]string, 0, len(weekdayTotal))
					valueArgs = make([]interface{}, 0, len(weekdayTotal)*2)
					for weekday, summary := range weekdayTotal[sourceid][yearbit][monthbit] {
						valueStrings = append(valueStrings, "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
						valueArgs = append(valueArgs, sourceid, yearbit, monthbit, weekday, category,
							summary[category].UniqueVisitors, summary[category].Visits, summary[category].KBytes,
							summary[category].Pages, summary[category].Hit, summary[category].NonBotPages,
							summary[category].NonBotHits)
					}
					stmt = fmt.Sprintf(
						`INSERT INTO weekday_total ( sourceid, year, month, weekday, category, uniquevisitors, visits, kbytes, pages, hit, notbotpage, notbothit) values %s`,
						strings.Join(valueStrings, ","))
					_, err = dbOfLogs.Exec(stmt, valueArgs...)
					if err != nil {
						fmt.Printf("Failed\n")
						log.Fatal(err)
					}
					// DATE TOTALS
					valueStrings = make([]string, 0, len(dateTotal))
					valueArgs = make([]interface{}, 0, len(dateTotal)*2)
					for date, summary := range dateTotal[sourceid][yearbit][monthbit] {
						valueStrings = append(valueStrings, "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
						valueArgs = append(valueArgs, sourceid, yearbit, monthbit, date, category,
							summary[category].UniqueVisitors, summary[category].Visits, summary[category].KBytes,
							summary[category].Pages, summary[category].Hit, summary[category].NonBotPages,
							summary[category].NonBotHits)
					}
					stmt = fmt.Sprintf(
						`INSERT INTO date_total ( sourceid, year, month, date, category, uniquevisitors, visits, kbytes, pages, hit, notbotpage, notbothit) values %s`,
						strings.Join(valueStrings, ","))
					_, err = dbOfLogs.Exec(stmt, valueArgs...)
					if err != nil {
						fmt.Printf("Failed\n")
						log.Fatal(err)
					}
				}
			}
		}
	}
	// Prepare for the next file
	unsavedRows = make([]StorageEntry, 0, batchSize)
	// monthSeenVisitors = make(map[int]map[int]map[int]string)
	// monthTotal = make(map[string]map[int]map[int]SummarySet)
	// ipTotal = make(map[int]map[int]map[string]int64)
}

// We've finished uploading this file, so update all the IP lookups
func FinaliseProcess(sourceid string) {
	// fmt.Printf("Event time %s\n", time.Now().String())
	// _, err := dbOfLogs.Exec(`UPDATE ip_total it
	// SET countrycode = (SELECT twola
	// 				FROM ip_lookup
	// 				WHERE SUBSTRING_INDEX(SUBSTRING_INDEX(it.ip, '.', 1), '.', -1) * 256 * 256 * 256 +
	// 				SUBSTRING_INDEX(SUBSTRING_INDEX(it.ip, '.', 2), '.', -1) * 256 * 256 +
	// 				SUBSTRING_INDEX(SUBSTRING_INDEX(it.ip, '.', 3), '.', -1) * 256 +
	// 				SUBSTRING_INDEX(SUBSTRING_INDEX(it.ip, '.', 4), '.', -1) BETWEEN min_ip AND max_ip ORDER BY min_ip LIMIT 1)
	// WHERE sourceid = ?`, sourceid)
	// if err != nil {
	// 	log.Fatal(err)
	// }
}
func CreateBaseTables() {
	var err error
	ConnectToDB()

	sqlStmt := `
	drop table if exists apache_entry;
	create table apache_entry (
		sourceid varchar(200),
		sourceline int,
		ipaddress varchar(20),
		identd varchar(50),
		username varchar(50),
		requestdateandtime datetime,
		requestmethod varchar(20),
		requestresource varchar(4000),
		requestprotocol varchar(20),
		statuscode int,
		bytessent int,
		referrer varchar(4000),
		useragent varchar(2000),
		country varchar(200),
		countrycode varchar(3),
		filetype varchar(150),
		operatingsystem varchar(200),
		browser varchar(200),
		isbot boolean,
		issearchengine boolean,
		ismobile boolean,
		istablet boolean,
		key(requestdateandtime)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8
	PARTITION BY RANGE( YEAR(requestdateandtime) ) (
		PARTITION p2008 VALUES LESS THAN (2009),
		PARTITION p2009 VALUES LESS THAN (2010),
		PARTITION p2010 VALUES LESS THAN (2011),
		PARTITION p2011 VALUES LESS THAN (2012),
		PARTITION p2012 VALUES LESS THAN (2013),
		PARTITION p2013 VALUES LESS THAN (2014),
		PARTITION p2014 VALUES LESS THAN (2015),
		PARTITION p2015 VALUES LESS THAN (2016),
		PARTITION p2016 VALUES LESS THAN (2017),
		PARTITION p2017 VALUES LESS THAN (2018),
		PARTITION p2018 VALUES LESS THAN (2019),
		PARTITION p2019 VALUES LESS THAN (2020),
		PARTITION p2020 VALUES LESS THAN (2021),
		PARTITION p2021 VALUES LESS THAN (2022),
		PARTITION p2022 VALUES LESS THAN (2023),
		PARTITION p2023 VALUES LESS THAN (2024),
		PARTITION p2024 VALUES LESS THAN (2025),
		PARTITION future VALUES LESS THAN MAXVALUE
	);
	create table ip_geo (
		ip          varchar(20) not null primary key,
		countrycode  varchar(3),
		countryname varchar(200)
	);
	create table ip_lookup (
		min_ip bigint,
		max_ip bigint,
		c1  varchar(200),
		c2  varchar(200),
		twola  varchar(2),
		threela varchar(3),
		countryname varchar(200),
		something varchar(300),
		somethingelse varchar(300));
	CREATE INDEX ip_lookup_min_ip_IDX ON ip_lookup (min_ip,max_ip);

	-- Summaries
	CREATE TABLE month_total (
		sourceid varchar(200),
		month int,
		year int,
		category varchar(30),
		uniquevisitors int,
		visits int,
		kbytes int,
		pages int,
		hit int,
		notbotpage int,
		notbothit int,
		primary key (month, year, category, sourceid)
	);
	CREATE TABLE hour_total (
		sourceid varchar(200),
		year int,
		month int,
		hour int, 
		category varchar(30),
		uniquevisitors int,
		visits int,
		kbytes int,
		pages int,
		hit int,
		notbotpage int,
		notbothit int,
		primary key (sourceid, year, month, hour, category)
	);
	CREATE TABLE date_total (
		sourceid varchar(200),
		year int,
		month int,
		date int, 
		category varchar(30),
		uniquevisitors int,
		visits int,
		kbytes int,
		pages int,
		hit int,
		notbotpage int,
		notbothit int,
		primary key (sourceid, year, month, date, category)
	);
	CREATE TABLE weekday_total (
		sourceid varchar(200),
		year int,
		month int,
		weekday int, 
		category varchar(30),
		uniquevisitors int,
		visits int,
		kbytes int,
		pages int,
		hit int,
		notbotpage int,
		notbothit int,
		primary key (sourceid, year, month, weekday, category)
	);
	CREATE TABLE ip_total (
		sourceid varchar(200),
		year int,
		month int,
		ip varchar(200), 
		uniquevisitors int,
		visits int,
		kbytes int,
		pages int,
		hits int,
		notbotpage int,
		notbothit int,
		countrycode  varchar(3),
		primary key (sourceid, year, month, ip)
	);
	
	DROP PROCEDURE if exists apache_entry_ip_lookup;
	DELIMITER $$
	CREATE PROCEDURE apache_entry_ip_lookup()
	BEGIN
		DECLARE rowcount INT;
		DECLARE ip_address VARCHAR(200);
		DECLARE ip_address_number BIGINT;
		DECLARE new_country VARCHAR(400);
		DECLARE new_countrycode VARCHAR(2);
	
		DECLARE cur_newaddress
			CURSOR FOR 
				SELECT DISTINCT ipaddress
				FROM apache_entry
				WHERE country = '';
	
		OPEN cur_newaddress;
	
		processEntries: LOOP
			FETCH cur_newaddress INTO ip_address;
	
			SELECT COUNT(*)
			INTO rowcount
			FROM ip_geo
			WHERE ip = ip_address;
			IF rowcount = 0 THEN -- Get the numerical value for ip_address
				SELECT SUBSTRING_INDEX(SUBSTRING_INDEX(ip_address, '.', 1), ',', -1) * 256 * 256 * 256 +
					SUBSTRING_INDEX(SUBSTRING_INDEX(ip_address, '.', 2), ',', -1) * 256 * 256 +
					SUBSTRING_INDEX(SUBSTRING_INDEX(ip_address, '.', 3), ',', -1) * 256 +
					SUBSTRING_INDEX(SUBSTRING_INDEX(ip_address, '.', 4), ',', -1)
				INTO ip_address_number
				FROM dual;
				
				SELECT twola, countryname 
				INTO new_countrycode, new_country 
				FROM (
					SELECT ip_address, twola, countryname
					FROM ip_lookup
					WHERE ip_address BETWEEN min_ip AND max_ip
					UNION
					SELECT ip_address, '??', 'Unknown'
				) Mep
				ORDER BY 2 DESC
				LIMIT 1;
				INSERT INTO ip_geo (ip, countrycode, countryname)
				VALUES (ip_address, new_countrycode, new_country);
			ELSE 
					SELECT twola, countryname
					INTO new_countrycode, new_country
					FROM ip_geo
					WHERE ip = ip_address;
			END IF;
			UPDATE apache_entry
			SET country = new_country,
			countrycode = new_countrycode
			WHERE ip = ip_address;
		END LOOP processEntries;
		
		CLOSE cur_newaddress;
	END $$
	DELIMITER ;

	-- Non Apache Entry one
	DROP PROCEDURE if exists ip_country_lookup;
	DELIMITER $$
	CREATE PROCEDURE ip_country_lookup(
	IN sourceidin varchar(200))
	BEGIN
		DECLARE rowcount INT;
		DECLARE ip_address VARCHAR(200);
		DECLARE ip_address_number BIGINT;
		DECLARE new_country VARCHAR(400);
		DECLARE new_countrycode VARCHAR(2);
	
		DECLARE cur_newaddress
			CURSOR FOR 
				SELECT DISTINCT ip
				FROM ip_total
				WHERE sourceid = sourceidin and (countrycode is null or countrycode = '??');
	
		OPEN cur_newaddress;
	
		processEntries: LOOP
			FETCH cur_newaddress INTO ip_address;
	
			SELECT COUNT(*)
			INTO rowcount
			FROM ip_geo
			WHERE ip = ip_address;
			IF rowcount = 0 THEN -- Get the numerical value for ip_address
				SELECT SUBSTRING_INDEX(SUBSTRING_INDEX(ip_address, '.', 1), '.', -1) * 256 * 256 * 256 +
					SUBSTRING_INDEX(SUBSTRING_INDEX(ip_address, '.', 2), '.', -1) * 256 * 256 +
					SUBSTRING_INDEX(SUBSTRING_INDEX(ip_address, '.', 3), '.', -1) * 256 +
					SUBSTRING_INDEX(SUBSTRING_INDEX(ip_address, '.', 4), '.', -1)
				INTO ip_address_number
				FROM dual;
			
					SET new_countrycode = '??';
					SET new_country = 'Dunno';
				
					SELECT twola, countryname
					INTO new_countrycode, new_country 
					FROM ip_lookup
					WHERE ip_address_number BETWEEN min_ip AND max_ip;		
	
					INSERT INTO ip_geo (ip, countrycode, countryname)
					VALUES (ip_address, new_countrycode, new_country);
			ELSE 
					SELECT countrycode, countryname
					INTO new_countrycode, new_country
					FROM ip_geo
					WHERE ip = ip_address;
			END IF;
			UPDATE ip_total
			SET countrycode = new_countrycode
			WHERE ip = ip_address;
		END LOOP processEntries;
		
		CLOSE cur_newaddress;
	END $$
	DELIMITER ;
	`
	_, err = dbOfLogs.Exec(sqlStmt)
	if err != nil {
		log.Printf("%q: %s\n", err, sqlStmt)
		return
	}
}
