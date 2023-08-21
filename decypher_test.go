package main

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pattfy/useragent"
	"github.com/stretchr/testify/assert"
)

func TestSaveEntryTwoHuman(t *testing.T) {
	ClearExistingSummaries("CMTest01")
	ClearExistingSummaries("CMTest02")
	userAgent := useragent.New("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.146 Safari/537.36")
	storageEntry := StorageEntry{
		SourceId:           "CMTest01",
		LineNumber:         1,
		IPAddress:          "127.0.0.1",
		Identd:             "-",
		User:               "-",
		RequestDateAndTime: time.Date(2021, time.Month(1), 1, 0, 0, 0, 0, time.Local),
		RequestMethod:      "GET",
		RequestResource:    "/blog",
		RequestProtocol:    "HTTP/1.1",
		StatusCode:         200,
		BytesSent:          "1234",
		Referrer:           "-",
		UserAgent:          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.146 Safari/537.36",
		Filetype:           "",
		OperatingSystem:    userAgent.Platform.Name(),
		Browser:            userAgent.Browser.Name(),
		IsBot:              userAgent.IsBot(),
		IsSearchengine:     userAgent.Bot.IsSearchEngine(),
		IsMobile:           userAgent.Device.IsMobile(),
		IsTablet:           userAgent.Device.IsTablet(),
		Category:           "blog",
	}
	SaveEntry(storageEntry)

	storageEntry = StorageEntry{
		SourceId:           "CMTest01",
		LineNumber:         2,
		IPAddress:          "127.0.0.1",
		Identd:             "-",
		User:               "-",
		RequestDateAndTime: time.Date(2021, time.Month(1), 1, 0, 0, 20, 0, time.Local),
		RequestMethod:      "GET",
		RequestResource:    "/blog",
		RequestProtocol:    "HTTP/1.1",
		StatusCode:         200,
		BytesSent:          "1234",
		Referrer:           "-",
		UserAgent:          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.146 Safari/537.36",
		Filetype:           "",
		OperatingSystem:    userAgent.Platform.Name(),
		Browser:            userAgent.Browser.Name(),
		IsBot:              userAgent.IsBot(),
		IsSearchengine:     userAgent.Bot.IsSearchEngine(),
		IsMobile:           userAgent.Device.IsMobile(),
		IsTablet:           userAgent.Device.IsTablet(),
		Category:           "blog",
	}
	SaveEntry(storageEntry)

	assert.Equal(t, 2, monthTotal["CMTest01"][2021][1]["blog"].Hit, "Hits did not match")
	assert.Equal(t, int64(2468), monthTotal["CMTest01"][2021][1]["blog"].KBytes, "KBytes did not match")
	assert.Equal(t, 2, monthTotal["CMTest01"][2021][1]["blog"].NonBotHits, "Non Bot Hits did not match")
	assert.Equal(t, 2, monthTotal["CMTest01"][2021][1]["blog"].NonBotPages, "Non Bot Pages did not match")
	assert.Equal(t, 2, monthTotal["CMTest01"][2021][1]["blog"].Pages, "Pages did not match")
	assert.Equal(t, "CMTest01", monthTotal["CMTest01"][2021][1]["blog"].SourceId, "SourceId did not match")
	assert.Equal(t, 1, monthTotal["CMTest01"][2021][1]["blog"].UniqueVisitors, "Unique Visitors did not match")
}
func TestSaveEntryTwoBot(t *testing.T) {
	ClearExistingSummaries("CMTest01")
	ClearExistingSummaries("CMTest02")
	userAgent := useragent.New("Mozilla/5.0 (compatible; SemrushBot/7~bl; +http://www.semrush.com/bot.html)")
	storageEntry := StorageEntry{
		SourceId:           "CMTest02",
		LineNumber:         1,
		IPAddress:          "127.0.0.1",
		Identd:             "-",
		User:               "-",
		RequestDateAndTime: time.Date(2021, time.Month(1), 1, 0, 0, 0, 0, time.Local),
		RequestMethod:      "GET",
		RequestResource:    "/",
		RequestProtocol:    "HTTP/1.1",
		StatusCode:         200,
		BytesSent:          "1234",
		Referrer:           "-",
		UserAgent:          "Mozilla/5.0 (compatible; SemrushBot/7~bl; +http://www.semrush.com/bot.html)",
		Filetype:           "",
		OperatingSystem:    userAgent.Platform.Name(),
		Browser:            userAgent.Browser.Name(),
		IsBot:              userAgent.IsBot(),
		IsSearchengine:     userAgent.Bot.IsSearchEngine(),
		IsMobile:           userAgent.Device.IsMobile(),
		IsTablet:           userAgent.Device.IsTablet(),
		Category:           "blog",
	}
	SaveEntry(storageEntry)

	storageEntry = StorageEntry{
		SourceId:           "CMTest02",
		LineNumber:         2,
		IPAddress:          "127.0.0.1",
		Identd:             "-",
		User:               "-",
		RequestDateAndTime: time.Date(2021, time.Month(1), 1, 0, 0, 20, 0, time.Local),
		RequestMethod:      "GET",
		RequestResource:    "/blog",
		RequestProtocol:    "HTTP/1.1",
		StatusCode:         200,
		BytesSent:          "1234",
		Referrer:           "-",
		UserAgent:          "Mozilla/5.0 (compatible; SemrushBot/7~bl; +http://www.semrush.com/bot.html)",
		Filetype:           "",
		OperatingSystem:    userAgent.Platform.Name(),
		Browser:            userAgent.Browser.Name(),
		IsBot:              userAgent.IsBot(),
		IsSearchengine:     userAgent.Bot.IsSearchEngine(),
		IsMobile:           userAgent.Device.IsMobile(),
		IsTablet:           userAgent.Device.IsTablet(),
		Category:           "blog",
	}
	SaveEntry(storageEntry)

	assert.Equal(t, 2, monthTotal["CMTest02"][2021][1]["blog"].Hit, "Hits did not match")
	assert.Equal(t, int64(2468), monthTotal["CMTest02"][2021][1]["blog"].KBytes, "KBytes did not match")
	assert.Equal(t, 0, monthTotal["CMTest02"][2021][1]["blog"].NonBotHits, "Non Bot Hits did not match")
	assert.Equal(t, 0, monthTotal["CMTest02"][2021][1]["blog"].NonBotPages, "Non Bot Pages did not match")
	assert.Equal(t, 2, monthTotal["CMTest02"][2021][1]["blog"].Pages, "Pages did not match")
	assert.Equal(t, "CMTest02", monthTotal["CMTest02"][2021][1]["blog"].SourceId, "SourceId did not match")
	assert.Equal(t, 1, monthTotal["CMTest02"][2021][1]["blog"].UniqueVisitors, "Unique Visitors did not match")

}
func TestSaveEntryThreeHuman(t *testing.T) {
	ClearExistingSummaries("CMTest01")
	ClearExistingSummaries("CMTest02")
	userAgent := useragent.New("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.146 Safari/537.36")

	storageEntry := StorageEntry{
		SourceId:           "CMTest01",
		LineNumber:         1,
		IPAddress:          "127.0.0.1",
		Identd:             "-",
		User:               "-",
		RequestDateAndTime: time.Date(2021, time.Month(1), 1, 0, 0, 0, 0, time.Local),
		RequestMethod:      "GET",
		RequestResource:    "/",
		RequestProtocol:    "HTTP/1.1",
		StatusCode:         200,
		BytesSent:          "1234",
		Referrer:           "-",
		UserAgent:          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.146 Safari/537.36",
		Filetype:           "",
		OperatingSystem:    userAgent.Platform.Name(),
		Browser:            userAgent.Browser.Name(),
		IsBot:              userAgent.IsBot(),
		IsSearchengine:     userAgent.Bot.IsSearchEngine(),
		IsMobile:           userAgent.Device.IsMobile(),
		IsTablet:           userAgent.Device.IsTablet(),
		Category:           "blog",
	}
	SaveEntry(storageEntry)

	storageEntry = StorageEntry{
		SourceId:           "CMTest01",
		LineNumber:         2,
		IPAddress:          "127.0.0.2",
		Identd:             "-",
		User:               "-",
		RequestDateAndTime: time.Date(2021, time.Month(1), 1, 0, 0, 20, 0, time.Local),
		RequestMethod:      "GET",
		RequestResource:    "/blog",
		RequestProtocol:    "HTTP/1.1",
		StatusCode:         200,
		BytesSent:          "1234",
		Referrer:           "-",
		UserAgent:          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.146 Safari/537.36",
		Filetype:           "",
		OperatingSystem:    userAgent.Platform.Name(),
		Browser:            userAgent.Browser.Name(),
		IsBot:              userAgent.IsBot(),
		IsSearchengine:     userAgent.Bot.IsSearchEngine(),
		IsMobile:           userAgent.Device.IsMobile(),
		IsTablet:           userAgent.Device.IsTablet(),
		Category:           "blog",
	}
	SaveEntry(storageEntry)
	filetype := filepath.Ext("/blog/media/2021/02/mep.jpg")
	mep := strings.Split(filetype, "?")
	if len(mep) > 1 {
		filetype = mep[0]
	}
	if len(filetype) > 20 {
		filetype = "?"
	}
	storageEntry = StorageEntry{
		SourceId:           "CMTest01",
		LineNumber:         2,
		IPAddress:          "127.0.0.3",
		Identd:             "-",
		User:               "-",
		RequestDateAndTime: time.Date(2021, time.Month(1), 2, 0, 0, 20, 0, time.Local),
		RequestMethod:      "GET",
		RequestResource:    "/blog/media/2021/02/mep.jpg",
		RequestProtocol:    "HTTP/1.1",
		StatusCode:         200,
		BytesSent:          "1234",
		Referrer:           "-",
		UserAgent:          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.146 Safari/537.36",
		Filetype:           filetype,
		OperatingSystem:    userAgent.Platform.Name(),
		Browser:            userAgent.Browser.Name(),
		IsBot:              userAgent.IsBot(),
		IsSearchengine:     userAgent.Bot.IsSearchEngine(),
		IsMobile:           userAgent.Device.IsMobile(),
		IsTablet:           userAgent.Device.IsTablet(),
		Category:           "blog",
	}
	SaveEntry(storageEntry)

	// Month
	assert.Equal(t, 3, monthTotal["CMTest01"][2021][1]["blog"].Hit, "Hits did not match")
	assert.Equal(t, int64(3702), monthTotal["CMTest01"][2021][1]["blog"].KBytes, "KBytes did not match")
	assert.Equal(t, 3, monthTotal["CMTest01"][2021][1]["blog"].NonBotHits, "Non Bot Hits did not match")
	assert.Equal(t, 2, monthTotal["CMTest01"][2021][1]["blog"].NonBotPages, "Non Bot Pages did not match")
	assert.Equal(t, 2, monthTotal["CMTest01"][2021][1]["blog"].Pages, "Pages did not match")
	assert.Equal(t, "CMTest01", monthTotal["CMTest01"][2021][1]["blog"].SourceId, "SourceId did not match")
	assert.Equal(t, 3, monthTotal["CMTest01"][2021][1]["blog"].UniqueVisitors, "Unique Visitors did not match")
	// Date
	assert.Equal(t, 2, dateTotal["CMTest01"][2021][1][1]["blog"].Hit, "DATE Hits did not match")
	assert.Equal(t, 1, dateTotal["CMTest01"][2021][1][2]["blog"].Hit, "DATE Hits did not match")
	assert.Equal(t, int64(2468), dateTotal["CMTest01"][2021][1][1]["blog"].KBytes, "DATE KBytes did not match")
	assert.Equal(t, int64(1234), dateTotal["CMTest01"][2021][1][2]["blog"].KBytes, "DATE KBytes did not match")
	assert.Equal(t, 2, dateTotal["CMTest01"][2021][1][1]["blog"].NonBotHits, "DATE Non Bot Hits did not match")
	assert.Equal(t, 2, dateTotal["CMTest01"][2021][1][1]["blog"].NonBotPages, "DATE Non Bot Pages did not match")
	assert.Equal(t, 2, dateTotal["CMTest01"][2021][1][1]["blog"].Pages, "DATE Pages did not match")
	assert.Equal(t, 2, dateTotal["CMTest01"][2021][1][1]["blog"].UniqueVisitors, "DATE Unique Visitors did not match")
	assert.Equal(t, "CMTest01", dateTotal["CMTest01"][2021][1][1]["blog"].SourceId, "DATE SourceId did not match")
	assert.Equal(t, 1, dateTotal["CMTest01"][2021][1][2]["blog"].NonBotHits, "DATE Non Bot Hits did not match")
	assert.Equal(t, 0, dateTotal["CMTest01"][2021][1][2]["blog"].NonBotPages, "DATE Non Bot Pages did not match")
	assert.Equal(t, 0, dateTotal["CMTest01"][2021][1][2]["blog"].Pages, "DATE Pages did not match")
	assert.Equal(t, 1, dateTotal["CMTest01"][2021][1][2]["blog"].UniqueVisitors, "DATE Unique Visitors did not match")
	assert.Equal(t, "CMTest01", dateTotal["CMTest01"][2021][1][2]["blog"].SourceId, "DATE SourceId did not match")

}
