package main

/*
 * Imports Apache log entries from files, tails or other methods
 */

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"time"
)

/**
 * This reads apache log files
 */

/*
141.226.212.148 - - [31/Jan/2021:23:17:11 +1100] "GET /blog/rss.xml HTTP/1.1" 200 24627 "-" "UniversalFeedParser/5.1.3 +https://code.google.com/p/feedparser/"
*/
// ApacheEntry is a line in an apache file
type ApacheEntry struct {
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
}

type ApacheDigest struct {
	LinkedLogFileHandle  string
	LinkedLogFileScanner *bufio.Scanner
	LinkedLogFileOpen    *os.File
}

func (ad ApacheDigest) SourceId() string {
	if ad.LinkedLogFileHandle != "" {
		return ad.LinkedLogFileHandle
	} else {
		return ""
	}
}

var apacheDigest ApacheDigest
var lineNumber int

func LinkLogfile(filename string) error {
	var err error
	apacheDigest.LinkedLogFileHandle = filepath.Base(filename)
	apacheDigest.LinkedLogFileOpen, err = os.Open(filename)
	lineNumber = 0
	if err != nil {
		return err
	}
	apacheDigest.LinkedLogFileScanner = bufio.NewScanner(apacheDigest.LinkedLogFileOpen)
	return err
}

func NextLine() (ApacheEntry, error) {
	lineNumber = lineNumber + 1
	ok := apacheDigest.LinkedLogFileScanner.Scan()
	layout := "02/Jan/2006:15:04:05 -0700"
	if ok {
		entryString := apacheDigest.LinkedLogFileScanner.Text()
		// 141.226.212.148 - - [31/Jan/2021:23:17:10 +1100] "GET /robots.txt HTTP/1.1" 200 - "-" "omgili/0.5 +http://omgili.com"
		matchExpression := regexp.MustCompile(`^(?P<IPAddress>\S+) (?P<Identd>\S+) (?P<User>\S+) \[(?P<RequestDateAndTime>[\w:/]+\s[+\-]\d{4})\] "(?P<MethodResourceProtocol>([^"]|\\")*)" (?P<StatusCode>\d{3}) (?P<BytesSent>\S+) "(?P<Referrer>([^"]|\\")*)" "(?P<UserAgent>(.*))"$`)
		results := matchExpression.FindStringSubmatch(entryString)

		matchExpression2 := regexp.MustCompile(`^(?P<RequestMethod>\S+) (?P<RequestResource>.+?)\s(?P<RequestProtocol>\S+)$`)
		// fmt.Printf("%d\n", lineNumber)
		if len(results) == 0 {
			fmt.Printf("Failed to match line\n File:%s\n Line:%d\n Entry:%s\n", apacheDigest.LinkedLogFileHandle, lineNumber, entryString)
			return ApacheEntry{}, fmt.Errorf("failed to match line: File:%s Line:%d Entry:%s", apacheDigest.LinkedLogFileHandle, lineNumber, entryString)
		} else {
			results2 := matchExpression2.FindStringSubmatch(results[matchExpression.SubexpIndex("MethodResourceProtocol")])
			requestmethod := ""
			requestresource := ""
			requestprotocol := ""
			if len(results2) > 0 {
				requestmethod = results2[matchExpression2.SubexpIndex("RequestMethod")]
				requestresource = results2[matchExpression2.SubexpIndex("RequestResource")]
				requestprotocol = results2[matchExpression2.SubexpIndex("RequestProtocol")]
			}
			apacheEntry := ApacheEntry{
				SourceId:        apacheDigest.SourceId(),
				LineNumber:      lineNumber,
				IPAddress:       results[matchExpression.SubexpIndex("IPAddress")],
				Identd:          results[matchExpression.SubexpIndex("Identd")],
				User:            results[matchExpression.SubexpIndex("User")],
				RequestMethod:   requestmethod,
				RequestResource: requestresource,
				RequestProtocol: requestprotocol,
				BytesSent:       results[matchExpression.SubexpIndex("BytesSent")],
				Referrer:        results[matchExpression.SubexpIndex("Referrer")],
				UserAgent:       results[matchExpression.SubexpIndex("UserAgent")],
			}
			apacheEntry.RequestDateAndTime, _ = time.Parse(layout, results[matchExpression.SubexpIndex("RequestDateAndTime")])
			apacheEntry.StatusCode, _ = strconv.Atoi(results[matchExpression.SubexpIndex("StatusCode")])
			return apacheEntry, nil
		}
	}
	return ApacheEntry{}, apacheDigest.LinkedLogFileScanner.Err()
}

func CloseLogFile() error {
	apacheDigest.LinkedLogFileOpen.Close()
	return nil
}

func updateDatabaseFromFile(nameOfFile string) {
	fmt.Printf("\nProcessing %s: ...", nameOfFile)
	LinkLogfile(nameOfFile)
	ClearExistingForSource(apacheDigest.SourceId())
	entry, err := NextLine()
	lastLineNumber := entry.LineNumber
	for err == nil && entry.LineNumber != 0 {
		ProcessEntry(entry)
		lastLineNumber = entry.LineNumber
		entry, err = NextLine()
	}
	SaveToDb() // Save remainders
	FinaliseProcess(apacheDigest.SourceId())
	fmt.Printf("saved %d\n", lastLineNumber)

}

func updateDatabaseFromFolder(basedir string) {
	basedir += "/"
	fmt.Printf("\nProcessing directory %s: ...\n", basedir)
	files, err := ioutil.ReadDir(basedir)
	if err != nil {
		log.Fatal(err)
	}
	for _, f := range files {
		nameOfFile := f.Name()
		if f.IsDir() ||
			(len(nameOfFile) > 2 && nameOfFile[len(nameOfFile)-2:] == "gz") ||
			(len(nameOfFile) > 4 && nameOfFile[len(nameOfFile)-4:] == "html") {
		} else {
			updateDatabaseFromFile(basedir + nameOfFile)
		}
	}

}
