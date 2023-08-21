# Apache Analysis

The 3D system

* Digest - acquiring and reading the blogs
* Decypher - converting the blogs into usable data caches
* Display - showing the results

## Digest

Currently pass it a log file, and it processes each line into an ApacheEntry struct

## Decypher

* Creates and maintains the database of summaries and data.
* Send it an ApacheEntry and it updates the data and summaries

### Decypher Targets

* Monthly totals of Unique Visitors, Visits, KBytes, Pages (non-media), Hits (all), Non-Bot Unique Visits, Non-Bot Pages
* Monthly Daily totals
* Monthly Day-of-week totals
* Monthly Hour totals
* Monthly top URL totals
    * By Hit count
    * By download size
* Monthly top entry / exit page totals
    * Not media, html/php only
* Monthly Country totals
* Monthly Robot totals
* Monthly non-Robot IP totals
* Monthly file-extension totals
* Monthly page totals
* Monthly OS Totals
* Monthly Browser totals
* Monthly Referrer totals
    * Break down by host, then by path.
* Monthly HttpCode totals
* Monthly 404 list
* Monthly strange Action list (not in GET/POST/HEAD)

Don't include non-200OK data in any of the totals.


## Display

* Takes the database of summaries and data
* Creates graphs and displays

# Go commands

## Packager

* `go get fyne.io/fyne/cmd/fyne`
* `fyne package -os android -appID my.domain.appname`
* `bin/fyne package -os darwin -icon icon.png -name "Apache Visualiser" -release ; cp Simple_world_map.png Apache\ Visualiser/Content/Resources ; cp country_points.json Apache\ Visualiser/Content/Resources`
* `bin/fyne package -os windows -icon icon.png -name "Apache Visualiser" -release -appID=vonexplaino.com.apachevisualiser`

## Building

* `go build -ldflags "-s -w"`

* `env GOOS=darwin GOARCH=amd64 go build -ldflags "-s -w"`
* `env GOOS=windows GOARCH=amd64 go build -ldflags "-s -w"`
* `env GOOS=windows GOARCH=amd64 CGO_ENABLED="1" CC="x86_64-w64-mingw32-gcc" go build -ldflags "-s -w"` (brew install mingw-w64)

# IDEAS

* [x] With the map, add lamps to each country and light them up when they're indicated. 
* [x] Add a filter to not include certain URLs in tables (e.g. the corner-images in main pages are decoration and shouldn't be counted as 'visits')
    * [x] Alternatively, have a list of Pages + Hits that are not Assets, and another list of Pages + Hits that _are_ assets.
* [ ] Click an image to pop up a table
* [x] Have buttons to swap the graph table between Month, Hour, Day, Date graphs for the month
* [x] Fix the prev/ next month button
* [xs] Make the upload file/ upload folder part of the interface rather than the start of the main func()
* [ ] Make the status an array so that multiple things can be happening at once
    * [ ] The status becomes a string message when one event, or a # of events that you can click on to pop up what's going on

# CODE STUFF

## Running

```sh
docker run --name=mk-mysql -p3306:3306 -v mysql-volume:/var/lib/mysql -e MYSQL_ROOT_PASSWORD=my-secret-pw -d mysql/mysql-server:8.0.20
```
## Building

```sh
go build -ldflags "-s -w"
```

# Scratch Pad

```mysql
DROP TRIGGER if exists apache_entry_ip_lookup;
DELIMITER $$
CREATE TRIGGER apache_entry_ip_lookup 
BEFORE INSERT
ON apache_entry FOR EACH ROW
BEGIN
    DECLARE rowcount INT;
    DECLARE ip_address VARCHAR(200);
    DECLARE ip_address_number BIGINT;
    DECLARE new_country VARCHAR(400);
    DECLARE new_countrycode VARCHAR(2);
    SET ip_address := new.ipaddress;
    SET new_country := new.country;
    IF new_country = '' THEN
        SELECT COUNT(*)
        INTO rowcount
        FROM ip_geo
        WHERE ip = ip_address;
        IF rowcount = 0 THEN
            -- Get the numerical value for ip_address
            SELECT SUBSTRING_INDEX(SUBSTRING_INDEX(ip_address, '.', 1), ',', -1) * 256 * 256 * 256 +
                    SUBSTRING_INDEX(SUBSTRING_INDEX(ip_address, '.', 2), ',', -1) * 256 * 256 +
                    SUBSTRING_INDEX(SUBSTRING_INDEX(ip_address, '.', 3), ',', -1) * 256 +
                    SUBSTRING_INDEX(SUBSTRING_INDEX(ip_address, '.', 4), ',', -1)
            INTO ip_address_number
            FROM dual;
            INSERT INTO ip_geo
            SELECT * FROM (
                SELECT ip_address, twola, countryname
                FROM ip_lookup
                WHERE min_ip <= ip_address_number
                    AND max_ip >= ip_address_number
                UNION
                SELECT ip_address, '??', 'Unknown'
            ) Mep
            ORDER BY 2 DESC;
        END IF;
        SELECT countrycode, countryname
        INTO new_countrycode, new_country
        FROM ip_geo
        WHERE ip = ip_address;
        SET new.countrycode = new_countrycode;
        SET new.country = new_country;
    END IF;
END $$
DELIMITER ;
```