package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"image"
	"image/color"
	"image/draw"
	"image/png"
	"log"
	"math"
	"strings"
	"sync"
	"time"

	fyne "fyne.io/fyne/v2"
	app "fyne.io/fyne/v2/app"
	"fyne.io/fyne/v2/canvas"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/dialog"
	"fyne.io/fyne/v2/layout"
	"fyne.io/fyne/v2/theme"
	"fyne.io/fyne/v2/widget"

	"github.com/anthonynsimon/bild/clone"
	chart "github.com/wcharczuk/go-chart/v2"
	"github.com/wcharczuk/go-chart/v2/drawing"
)

func lineGraphSummaryCategories(xValues []map[string]SummaryBlock, keys []string, startWith float64) *canvas.Image {
	chart.DefaultBackgroundColor = chart.ColorTransparent
	chart.DefaultCanvasColor = chart.ColorTransparent
	var allChartSeries []chart.Series
	ticks := make([]chart.Tick, len(xValues))

	min := 2
	max := 0
	for k := range xValues {
		ticks[k] = chart.Tick{Value: float64(k), Label: fmt.Sprintf("%d", k+int(startWith))}
		if k < min {
			min = k
		}
		if k > max {
			max = k
		}
	}
	endWith := float64(len(xValues)) - 1
	maxValue := 0.0
	byteMaxValue := 0.0
	baseColors := make(map[string]drawing.Color, 5)
	baseColors["base"] = drawing.Color{R: 255, G: 0, B: 0, A: 255}
	baseColors["blog"] = drawing.Color{R: 0, G: 255, B: 0, A: 255}
	baseColors["media"] = drawing.Color{R: 0, G: 0, B: 255, A: 255}
	baseColors["theme"] = drawing.Color{R: 255, G: 0, B: 255, A: 255}
	baseColors["code"] = drawing.Color{R: 0, G: 255, B: 255, A: 255}

	for category := range baseColors { // Each category
		myColor := baseColors[category]
		for _, fieldName := range keys { // Each key
			disColor := myColor
			disWidth := 1.0
			disDash := []float64{}

			ySeriesUnique := make([]float64, len(xValues))
			seriesTitle := category
			for mep := min; mep <= max; mep++ {
				disValue := 0
				rest := xValues[mep][category]
				switch fieldName {
				case "uniquevisitors":
					disValue = rest.uniquevisitors
				case "visits":
					disValue = rest.visits
				case "kbytes":
					disValue = rest.kbytes
				case "pages":
					disValue = rest.pages
				case "hit":
					disValue = rest.hit
				case "notbotpage":
					disValue = rest.notbotpage
					disWidth = 3.0
					disDash = []float64{2.0, 2.0}
				case "notbothit":
					disValue = rest.notbothit
					disWidth = 3.0
					disDash = []float64{2.0, 2.0}
				}
				ySeriesUnique[mep] = float64(disValue)
			}
			yAxis := chart.YAxisPrimary
			if fieldName == "kbytes" {
				yAxis = chart.YAxisSecondary
			}
			allChartSeries = append(allChartSeries, chart.ContinuousSeries{
				Name:    seriesTitle + " " + fieldName,
				YAxis:   yAxis,
				XValues: chart.Seq{Sequence: chart.NewLinearSequence().WithStart(0).WithEnd(endWith)}.Values(),
				YValues: ySeriesUnique,
				Style:   chart.Style{StrokeColor: disColor, StrokeWidth: disWidth, StrokeDashArray: disDash},
			})
		}
	}

	graph := chart.Chart{
		Series: allChartSeries,
		XAxis:  chart.XAxis{Ticks: ticks},
		YAxis: chart.YAxis{
			Range: &chart.ContinuousRange{Min: 0.0, Max: maxValue},
			ValueFormatter: func(v interface{}) string {
				if vf, isFloat := v.(float64); isFloat {
					return fmt.Sprintf("%0.f", vf)
				}
				return ""
			},
		},
		YAxisSecondary: chart.YAxis{
			Range: &chart.ContinuousRange{Min: 0.0, Max: byteMaxValue},
			ValueFormatter: func(v interface{}) string {
				if vf, isFloat := v.(float64); isFloat {
					return fmt.Sprintf("%0.1fM", math.Floor(vf*10/1024)/10)
				}
				return ""
			},
		},
	}
	graph.Elements = []chart.Renderable{
		chart.Legend(&graph),
	}

	buffer := bytes.NewBuffer([]byte{})
	_ = graph.Render(chart.PNG, buffer)

	graph2 := canvas.NewImageFromResource(&fyne.StaticResource{
		StaticName:    "graph.png",
		StaticContent: buffer.Bytes()})
	graph2.ScaleMode = canvas.ImageScaleFastest
	graph2.SetMinSize(fyne.Size{Width: 200, Height: 200})
	graph2.FillMode = canvas.ImageFillContain
	return graph2
}

func barGraphSummary(xValues []map[string]SummaryBlock, keys []string, startWith float64) *canvas.Image {
	chart.DefaultBackgroundColor = chart.ColorTransparent
	chart.DefaultCanvasColor = chart.ColorTransparent

	var (
		colorWhite = drawing.Color{R: 241, G: 241, B: 241, A: 255}
		maxValue   = 0
	)
	baseColors := make(map[string]drawing.Color, 5)
	baseColors["base"] = drawing.Color{R: 255, G: 0, B: 0, A: 255}
	baseColors["blog"] = drawing.Color{R: 0, G: 255, B: 0, A: 255}
	baseColors["media"] = drawing.Color{R: 0, G: 0, B: 255, A: 255}
	baseColors["theme"] = drawing.Color{R: 255, G: 255, B: 0, A: 255}
	baseColors["code"] = drawing.Color{R: 0, G: 255, B: 255, A: 255}

	barChart := chart.BarChart{
		Title: "Monthly values",
		Background: chart.Style{
			Padding: chart.Box{
				Top: 100,
			},
		},
		BarSpacing: 1,
	}

	if len(xValues) == 0 || len(xValues[1]) == 0 {

		buffer := bytes.NewBuffer([]byte{})
		_ = barChart.Render(chart.PNG, buffer)

		graph2 := canvas.NewImageFromResource(&fyne.StaticResource{
			StaticName:    "graph.png",
			StaticContent: buffer.Bytes()})
		graph2.ScaleMode = canvas.ImageScaleFastest
		graph2.SetMinSize(fyne.Size{Width: 200, Height: 200})
		graph2.FillMode = canvas.ImageFillContain
		return graph2
	}

	var bars []chart.Value
	for _, someMore := range xValues { // Each day
		for category, rest := range someMore { // Each category
			myColor := baseColors[category]
			for _, fieldName := range keys { // Each key
				disValue := 0
				disColor := myColor
				switch fieldName {
				case "uniquevisitors":
					disValue = rest.uniquevisitors
				case "visits":
					disValue = rest.visits
					disColor.R -= 30
				case "kbytes":
					disValue = rest.kbytes
					disColor.B -= 30
				case "pages":
					disValue = rest.pages
					disColor.G -= 30
				case "hit":
					disValue = rest.hit
					disColor.R -= 30
					disColor.B -= 30
				case "notbotpage":
					disValue = rest.notbotpage
					disColor.R -= 30
					disColor.G -= 30
				case "notbothit":
					disValue = rest.notbothit
					disColor.G -= 30
					disColor.B -= 30
				}
				if maxValue < disValue {
					maxValue = disValue
				}
				bars = append(bars, chart.Value{
					Label: "",
					Value: float64(disValue),
					Style: chart.Style{
						StrokeWidth: .01,
						FillColor:   disColor,
						FontColor:   colorWhite,
					},
				})
			}
		}
	}
	fmt.Printf("Number of bars %d\n", len(bars))

	barChart.Bars = bars
	barChart.YAxis = chart.YAxis{
		Range: &chart.ContinuousRange{Min: 0.0, Max: float64(maxValue)},
		ValueFormatter: func(v interface{}) string {
			if vf, isFloat := v.(float64); isFloat {
				return fmt.Sprintf("%0.f", vf)
			}
			return ""
		},
	}
	// Turn chart object into PNG for display.
	buffer := bytes.NewBuffer([]byte{})
	_ = barChart.Render(chart.PNG, buffer)

	graph2 := canvas.NewImageFromResource(&fyne.StaticResource{
		StaticName:    "graph.png",
		StaticContent: buffer.Bytes()})
	graph2.ScaleMode = canvas.ImageScaleFastest
	graph2.SetMinSize(fyne.Size{Width: 200, Height: 200})
	graph2.FillMode = canvas.ImageFillContain
	return graph2
}

func lineGraphSummary(xValues []SummaryBlock, keys []string, startWith float64) *canvas.Image {
	var allChartSeries []chart.Series
	ticks := make([]chart.Tick, len(xValues))

	min := 2
	max := 0
	for k := range xValues {
		ticks[k] = chart.Tick{Value: float64(k), Label: fmt.Sprintf("%d", k+int(startWith))}
		if k < min {
			min = k
		}
		if k > max {
			max = k
		}
	}
	endWith := float64(len(xValues)) - 1

	maxValue := 0.0
	byteMaxValue := 0.0

	for _, fieldName := range keys {
		ySeriesUnique := make([]float64, len(xValues))
		seriesTitle := "Unknown"
		for mep := min; mep <= max; mep++ {
			var appender float64
			switch fieldName {
			case "uniquevisitors":
				appender = float64(xValues[mep].uniquevisitors)
				seriesTitle = "Unique Visitors"
			case "visits":
				appender = float64(xValues[mep].visits)
				seriesTitle = "Visits"
			case "kbytes":
				appender = float64(xValues[mep].kbytes)
				seriesTitle = "Kbytes"
			case "pages":
				appender = float64(xValues[mep].pages)
				seriesTitle = "Pages"
			case "hit":
				appender = float64(xValues[mep].hit)
				seriesTitle = "Hits"
			case "notbotpage":
				appender = float64(xValues[mep].notbotpage)
				seriesTitle = "Not bot pages"
			case "notbothit":
				appender = float64(xValues[mep].notbothit)
				seriesTitle = "Not bot hits"
			}
			if fieldName == "kbytes" {
				if appender > byteMaxValue {
					byteMaxValue = appender
				}
			} else if appender > maxValue {
				maxValue = appender
			}
			ySeriesUnique[mep] = appender
		}
		yAxis := chart.YAxisPrimary
		if fieldName == "kbytes" {
			yAxis = chart.YAxisSecondary
		}
		allChartSeries = append(allChartSeries, chart.ContinuousSeries{
			Name:    seriesTitle,
			YAxis:   yAxis,
			XValues: chart.Seq{Sequence: chart.NewLinearSequence().WithStart(0).WithEnd(endWith)}.Values(),
			YValues: ySeriesUnique,
		})
	}

	graph := chart.Chart{
		Series: allChartSeries,
		XAxis:  chart.XAxis{Ticks: ticks},
		YAxis: chart.YAxis{
			Range: &chart.ContinuousRange{Min: 0.0, Max: maxValue},
			ValueFormatter: func(v interface{}) string {
				if vf, isFloat := v.(float64); isFloat {
					return fmt.Sprintf("%0.f", vf)
				}
				return ""
			},
		},
		YAxisSecondary: chart.YAxis{
			Range: &chart.ContinuousRange{Min: 0.0, Max: byteMaxValue},
			ValueFormatter: func(v interface{}) string {
				if vf, isFloat := v.(float64); isFloat {
					return fmt.Sprintf("%0.1fM", math.Floor(vf*10/1024/1024)/10)
				}
				return ""
			},
		},
	}
	graph.Elements = []chart.Renderable{
		chart.Legend(&graph),
	}

	buffer := bytes.NewBuffer([]byte{})
	_ = graph.Render(chart.PNG, buffer)

	graph2 := canvas.NewImageFromResource(&fyne.StaticResource{
		StaticName:    "graph.png",
		StaticContent: buffer.Bytes()})
	graph2.ScaleMode = canvas.ImageScaleFastest
	graph2.SetMinSize(fyne.Size{Width: 200, Height: 200})
	graph2.FillMode = canvas.ImageFillContain
	return graph2
}

func drawCircle(img *image.RGBA, x0, y0, r int, c color.Color) {
	x, y, dx, dy := r-1, 0, 1, 1
	err := dx - (r * 2)

	for x > y {
		img.Set(x0+x, y0+y, c)
		img.Set(x0+y, y0+x, c)
		img.Set(x0-y, y0+x, c)
		img.Set(x0-x, y0+y, c)
		img.Set(x0-x, y0-y, c)
		img.Set(x0-y, y0-x, c)
		img.Set(x0+y, y0-x, c)
		img.Set(x0+x, y0-y, c)

		if err <= 0 {
			y++
			err += dy
			dy += 2
		}
		if err > 0 {
			x--
			dx += 2
			err += dx - (r * 2)
		}
	}
}

func worldGraphPng(year int, month int) *canvas.Image {
	// Values
	values := make(map[string]int64)
	var orderOfKeys []string
	res, err := dbOfLogs.Query(CountryQuery, year, month)
	if err != nil {
		log.Fatal(err)
	}
	defer res.Close()
	for res.Next() {
		var aRow Countrycount
		err := res.Scan(&aRow.CountryCode, &aRow.Count)

		if err != nil {
			log.Fatal(err)
		}
		values[aRow.CountryCode] = int64(aRow.Count)
		// Since for ... range doesn't respect order (in fact
		// intentionally randomises), you have to store order
		// of values separately.
		orderOfKeys = append(orderOfKeys, aRow.CountryCode)
	}
	var jsonFile = make(map[string][][]int)
	err = json.Unmarshal(countryPoints, &jsonFile)
	if err != nil {
		panic(err)
	}

	baseMap, err := png.Decode(base64.NewDecoder(base64.StdEncoding, strings.NewReader(imageWorldMap)))
	if err != nil {
		panic(err)
	}
	cimg := image.NewRGBA(baseMap.Bounds())

	i := 0
	color := color.RGBA{0, 0, 0, 255}
	for _, key := range orderOfKeys {
		i++
		if i < 5 {
			color.R = 255
			color.G = 215
			color.B = 0
		} else if i < 15 {
			color.R = 240
			color.G = 248
			color.B = 255
		} else if i < 30 {
			color.R = 205
			color.G = 149
			color.B = 117
		} else {
			break
		}
		for _, point := range jsonFile[key] {
			if len(point) > 2 && point[2] == 1 {
				baseMap = FloodFill(baseMap, image.Point{point[0], point[1]}, color)
			} else {
				drawCircle(cimg, point[0], point[1], 1, color)
				drawCircle(cimg, point[0], point[1], 2, color)
				drawCircle(cimg, point[0], point[1], 3, color)
				drawCircle(cimg, point[0], point[1], 4, image.Black)
				drawCircle(cimg, point[0], point[1], 5, image.Black)
			}
		}
	}
	cimg2 := image.NewRGBA(baseMap.Bounds())
	draw.Draw(cimg2, baseMap.Bounds(), baseMap, image.Point{}, draw.Over)
	draw.Draw(cimg2, cimg.Bounds(), cimg, image.Point{}, draw.Over)
	mapImage := canvas.NewImageFromImage(cimg2)
	mapImage.ScaleMode = canvas.ImageScaleFastest
	mapImage.SetMinSize(fyne.Size{Width: 495, Height: 266})
	mapImage.FillMode = canvas.ImageFillContain
	return mapImage
}

var thisApp fyne.App
var mainWindow fyne.Window
var toolbarCanvas *widget.Toolbar
var statusBarContent *fyne.Container
var statusBarLeft *widget.Label
var statusBarRight *widget.Label
var currentDataFor time.Time

type Countrycount struct {
	CountryCode string
	Count       int
}

const CountryQuery string = `select mep.code, mep.total from (select ifnull(countrycode,'??') as code, count(*) as total from ip_total where year = ? and month = ? group by countrycode order by total desc) mep limit 30`
const DateQuery string = `select date as dt, sum(uniquevisitors) as uniquevisitors,
sum(visits) as visits,
sum(kbytes) as kbytes,
sum(pages) as pages,
sum(hit) as hit,
sum(notbotpage) as notbotpage,
sum(notbothit) as notbothit from date_total where year = ? and month = ?
group by date`
const DateCategoryQuery string = `select date as dt, category as cat, sum(uniquevisitors) as uniquevisitors,
sum(visits) as visits,
sum(kbytes) as kbytes,
sum(pages) as pages,
sum(hit) as hit,
sum(notbotpage) as notbotpage,
sum(notbothit) as notbothit from date_total where year = ? and month = ?
group by date, category`
const WeekdayQuery string = `select weekday as dy, sum(uniquevisitors) as uniquevisitors,
sum(visits) as visits,
sum(kbytes) as kbytes,
sum(pages) as pages,
sum(hit) as hit,
sum(notbotpage) as notbotpage,
sum(notbothit) as notbothit from weekday_total where year = ? and month = ?
group by weekday`
const HourQuery string = `select hour as hr, sum(uniquevisitors) as uniquevisitors,
sum(visits) as visits,
sum(kbytes) as kbytes,
sum(pages) as pages,
sum(hit) as hit,
sum(notbotpage) as notbotpage,
sum(notbothit) as notbothit from hour_total where year = ? and month = ?
group by hour`

type SummaryBlock struct {
	uniquevisitors int
	visits         int
	pages          int
	kbytes         int
	notbotpage     int
	hit            int
	notbothit      int
}

func getMonthValuesByDate(year, month int) []SummaryBlock {
	returnThis := make([]SummaryBlock, 32)
	res, err := dbOfLogs.Query(DateQuery, year, month)
	if err != nil {
		log.Fatal(err)
	}
	defer res.Close()
	for res.Next() {
		var aRow SummaryBlock
		var dt int
		err := res.Scan(&dt, &aRow.uniquevisitors, &aRow.visits, &aRow.kbytes, &aRow.pages, &aRow.hit, &aRow.notbotpage, &aRow.notbothit)

		if err != nil {
			log.Fatal(err)
		}
		returnThis[dt] = aRow
		// Since for ... range doesn't respect order (in fact
		// intentionally randomises), you have to store order
		// of values separately.
	}
	return returnThis[1:(time.Date(year, time.Month(month+1), 0, 0, 0, 0, 0, time.Local).Day() + 1)]
}
func getMonthValuesByDateAndCategory(year, month int) []map[string]SummaryBlock {
	returnThis := make([]map[string]SummaryBlock, 32)
	for i := range returnThis {
		returnThis[i] = make(map[string]SummaryBlock, 5)
	}
	res, err := dbOfLogs.Query(DateCategoryQuery, year, month)
	if err != nil {
		log.Fatal(err)
	}
	defer res.Close()
	for res.Next() {
		var aRow SummaryBlock
		var dt int
		var cat string
		err := res.Scan(&dt, &cat, &aRow.uniquevisitors, &aRow.visits, &aRow.kbytes, &aRow.pages, &aRow.hit, &aRow.notbotpage, &aRow.notbothit)

		if err != nil {
			log.Fatal(err)
		}
		returnThis[dt][cat] = aRow
	}
	return returnThis[1:(time.Date(year, time.Month(month+1), 0, 0, 0, 0, 0, time.Local).Day() + 1)]
}

func getHourValuesByDate(year, month int) []SummaryBlock {
	returnThis := make([]SummaryBlock, 24)
	res, err := dbOfLogs.Query(HourQuery, year, month)
	if err != nil {
		log.Fatal(err)
	}
	defer res.Close()
	for res.Next() {
		var aRow SummaryBlock
		var hr int
		err := res.Scan(&hr, &aRow.uniquevisitors, &aRow.visits, &aRow.kbytes, &aRow.pages, &aRow.hit, &aRow.notbotpage, &aRow.notbothit)

		if err != nil {
			log.Fatal(err)
		}
		returnThis[hr] = aRow
		// Since for ... range doesn't respect order (in fact
		// intentionally randomises), you have to store order
		// of values separately.
	}
	return returnThis[0:24]
}

func getWeekdayValuesByDate(year, month int) []SummaryBlock {
	returnThis := make([]SummaryBlock, 7)
	res, err := dbOfLogs.Query(WeekdayQuery, year, month)
	if err != nil {
		log.Fatal(err)
	}
	defer res.Close()
	for res.Next() {
		var aRow SummaryBlock
		var dy int
		err := res.Scan(&dy, &aRow.uniquevisitors, &aRow.visits, &aRow.kbytes, &aRow.pages, &aRow.hit, &aRow.notbotpage, &aRow.notbothit)

		if err != nil {
			log.Fatal(err)
		}
		returnThis[dy] = aRow
		// Since for ... range doesn't respect order (in fact
		// intentionally randomises), you have to store order
		// of values separately.
	}
	return returnThis[0:7]
}

func moveDateTo(currentDataFor time.Time, containerHolder *fyne.Container, lineGraphHolder *fyne.Container, barGraphHolder *fyne.Container) {
	statusBarLeft.SetText(currentDataFor.Format("Jan 2006"))
	statusBarLeft.Refresh()
	statusBarRight.SetText(currentDataFor.Format("Loading..."))
	statusBarRight.Refresh()
	summaryByDate := getMonthValuesByDate(currentDataFor.Year(), int(currentDataFor.Month()))
	var waitGroup sync.WaitGroup
	waitGroup.Add(2)
	go func(containerHolder *fyne.Container) {
		defer waitGroup.Done()
		newMap := worldGraphPng(currentDataFor.Year(), int(currentDataFor.Month()))
		containerHolder.Objects[1] = newMap
	}(containerHolder)
	go func(containerHolder *fyne.Container) {
		defer waitGroup.Done()
		lineGraphHolder.Objects[1] = lineGraphSummary(summaryByDate, []string{"uniquevisitors", "pages", "kbytes"}, 1) // , "notbotvisits"})
		summaryByDate2 := getMonthValuesByDateAndCategory(currentDataFor.Year(), int(currentDataFor.Month()))
		barGraphHolder.Objects[0] = lineGraphSummaryCategories(summaryByDate2, []string{"hit", "notbothit"}, 1)
	}(containerHolder)
	waitGroup.Wait()
	statusBarRight.SetText(currentDataFor.Format("IDLE"))
	statusBarRight.Refresh()
}

func displayMainWindow() {
	// Set "Current Date For" to the first of this month to make dancing back and forward easier.
	currentDataFor = time.Date(time.Now().Year(), time.Now().Month(), 1, 0, 0, 0, 0, time.Local)

	thisApp = app.NewWithID("Log Visualiser")
	mainWindow = thisApp.NewWindow("Log Visualiser")
	mainWindow.SetMaster()

	var summaryByDate []SummaryBlock
	var summaryByHour []SummaryBlock

	toolBarThisDude := widget.NewToolbar()
	blank := container.New(layout.NewCenterLayout())

	lineGraphHolder := container.New(
		layout.NewBorderLayout(toolBarThisDude, nil, nil, nil),
		toolBarThisDude,
		blank,
	)
	barGraphHolder := container.New(
		layout.NewBorderLayout(nil, nil, nil, nil),
		blank,
	)
	toolBarThisDude.Append(widget.NewToolbarAction(
		resourceCalendarMonthPng,
		func() {
			summaryByDate = getMonthValuesByDate(currentDataFor.Year(), int(currentDataFor.Month()))
			lineGraphHolder.Objects[1] = lineGraphSummary(summaryByDate, []string{"uniquevisitors", "pages", "kbytes"}, 1)
			lineGraphHolder.Refresh()
		},
	))
	toolBarThisDude.Append(widget.NewToolbarAction(
		resourceCalendarWeekdayPng,
		func() {
			lineGraphHolder.Objects[1] = lineGraphSummary(getWeekdayValuesByDate(currentDataFor.Year(), int(currentDataFor.Month())), []string{"uniquevisitors", "pages", "kbytes"}, 0)
			lineGraphHolder.Refresh()
		},
	))
	toolBarThisDude.Append(widget.NewToolbarAction(
		resourceClockPng,
		func() {
			summaryByHour = getHourValuesByDate(currentDataFor.Year(), int(currentDataFor.Month()))
			lineGraphHolder.Objects[1] = lineGraphSummary(summaryByHour, []string{"uniquevisitors", "pages", "kbytes"}, 0)
			lineGraphHolder.Refresh()
		},
	))
	toolBarThisDude.Refresh()
	containerHolder := container.New(
		layout.NewGridLayout(2),
		lineGraphHolder,
		blank,
		barGraphHolder,
		blank)

	toolbarCanvas = widget.NewToolbar(
		widget.NewToolbarAction(
			theme.NavigateBackIcon(),
			func() {
				currentDataFor = time.Date(currentDataFor.Year(), currentDataFor.Month(), 0, 0, 0, 0, 0, time.Local)
				// Hard specify to the first of this new month so we don't get math-add weirdness.
				currentDataFor = time.Date(currentDataFor.Year(), currentDataFor.Month(), 1, 0, 0, 0, 0, time.Local)
				moveDateTo(currentDataFor, containerHolder, lineGraphHolder, barGraphHolder)
			},
		),
		widget.NewToolbarAction(
			theme.HomeIcon(),
			func() {
				currentDataFor = time.Now()
				currentDataFor = time.Date(currentDataFor.Year(), currentDataFor.Month(), 0, 0, 0, 0, 0, time.Local)
				currentDataFor = time.Date(currentDataFor.Year(), currentDataFor.Month(), 1, 0, 0, 0, 0, time.Local)
				moveDateTo(currentDataFor, containerHolder, lineGraphHolder, barGraphHolder)
			},
		),
		widget.NewToolbarAction(
			theme.NavigateNextIcon(),
			func() {
				currentDataFor = currentDataFor.AddDate(0, 1, 0)
				moveDateTo(currentDataFor, containerHolder, lineGraphHolder, barGraphHolder)
				statusBarRight.Refresh()
			},
		),
		widget.NewToolbarSeparator(),
		widget.NewToolbarAction(
			theme.FileIcon(),
			func() {
				dialog.ShowFileOpen(
					func(lu fyne.URIReadCloser, e error) {
						go updateDatabaseFromFile(lu.URI().Path())
					},
					mainWindow)
			},
		),
		widget.NewToolbarAction(
			theme.FolderIcon(),
			func() {
				dialog.ShowFolderOpen(
					func(lu fyne.ListableURI, e error) {
						go updateDatabaseFromFolder(lu.Path())
					},
					mainWindow)
			},
		),
		widget.NewToolbarSeparator(),
		widget.NewToolbarAction(
			theme.SettingsIcon(),
			func() { fmt.Printf("Todo: Settings") },
		),
		widget.NewToolbarAction(
			theme.HelpIcon(),
			func() { fmt.Printf("Todo: Help") },
		),
	)
	statusBarLeft = widget.NewLabel(currentDataFor.Format("Jan 2006"))
	statusBarRight = widget.NewLabel("IDLE")
	statusBarContent = container.New(
		layout.NewHBoxLayout(),
		statusBarLeft,
		layout.NewSpacer(),
		statusBarRight,
	)

	mainContainer := container.New(
		layout.NewBorderLayout(
			toolbarCanvas,
			statusBarContent,
			nil,
			nil,
		),
		toolbarCanvas,
		statusBarContent,
		containerHolder,
	)
	mainWindow.SetContent(mainContainer)
	mainWindow.Show()

	summaryByDate2 := getMonthValuesByDateAndCategory(currentDataFor.Year(), int(currentDataFor.Month()))
	summaryByDate = getMonthValuesByDate(currentDataFor.Year(), int(currentDataFor.Month()))
	go func(containerHolder *fyne.Container) {
		newMap := worldGraphPng(currentDataFor.Year(), int(currentDataFor.Month()))
		containerHolder.Objects[1] = newMap
	}(containerHolder)
	go func(containerHolder, lineGraphHolder *fyne.Container) {
		toolBarThisDude.Refresh()
		lineGraphHolder.Objects[1] = lineGraphSummary(summaryByDate, []string{"uniquevisitors", "pages", "kbytes", "visits", "notbotpage", "notbothit"}, 1) // , "notbotvisits"})
		barGraphHolder.Objects[0] = lineGraphSummaryCategories(summaryByDate2, []string{"hit", "notbothit"}, 1)
		lineGraphHolder.Refresh()
		containerHolder.Refresh()
	}(containerHolder, lineGraphHolder)
	mainWindow.ShowAndRun()
}

// Based on code from https://codereview.stackexchange.com/questions/123581/golang-flood-fill
var mods = [...]struct {
	x, y int
}{
	{-1, 0}, {1, 0}, {0, -1}, {0, 1},
}

func FloodFill(img image.Image, sp image.Point, c color.Color) *image.RGBA {
	im := clone.AsRGBA(img)
	cR, cG, cB, cA := im.At(sp.X, sp.Y).RGBA()
	maxX := im.Bounds().Dx() - 1
	maxY := im.Bounds().Dy() - 1
	if sp.X > maxX || sp.X < 0 || sp.Y > maxY || sp.Y < 0 {
		return im
	}

	seen := make([][]bool, maxY)
	for i := 0; i < maxY; i++ {
		seen[i] = make([]bool, maxX)
	}

	// go will shuffle memory too when adding/removing items from q
	q := []image.Point{sp}

	for len(q) > 0 {

		// shift the q
		op := q[0]
		q = q[1:]

		if seen[op.Y][op.X] {
			continue
		}

		seen[op.Y][op.X] = true
		im.Set(op.X, op.Y, c)

		for _, mod := range mods {
			newx := op.X + mod.x
			newy := op.Y + mod.y
			if 0 <= newy && newy < maxY && 0 <= newx && newx < maxX {
				nR, nG, nB, nA := img.At(newx, newy).RGBA()
				if cR == nR && cG == nG && cB == nB && cA == nA {
					q = append(q, image.Point{X: newx, Y: newy})
				}
			}
		}
	}

	return im
}
