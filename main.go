package main

import (
	"archive/zip"
	"bytes"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"slices"
	"strings"
	"test/logging"
	"test/s3"
	"test/url"

	_ "github.com/duckdb/duckdb-go/v2"
)

func main() {
	logging.InitLogger("info")

	redirectURL, err := url.GetRedirectURL(os.Getenv("GTFS_FP_ENDPOINT"))

	if err != nil {
		slog.Error("Could not get redirect URL with error", "err", err)
		os.Exit(1)
	}

	client := &http.Client{}

	// Extract feed version from the redirect URL
	splitUrlParts := strings.Split(redirectURL, "/")
	fileName := splitUrlParts[len(splitUrlParts)-1]

	feedVersion := strings.Split(strings.ReplaceAll(fileName, ".zip", ""), "_")[2]

	dbFileName := fmt.Sprintf("%s_feed.db", feedVersion)
	zipFileName := fmt.Sprintf("%s_feed.zip", feedVersion)

	slog.Info("Determined feed version", "feedVersion", feedVersion)

	// Check if the feed already exists in S3
	feedExists, err := s3.CheckS3ObjectExists(
		os.Getenv("S3_ENDPOINT"),
		os.Getenv("S3_ACCESS_KEY"),
		os.Getenv("S3_SECRET_KEY"),
		"gtfs-fp",
		zipFileName,
	)

	if err != nil {
		slog.Error("Error checking if feed exists in S3", "err", err)
		os.Exit(1)
	}

	// Exit if the feed already exists
	if feedExists {
		slog.Info("Feed already exists in S3, exiting", "feedVersion", feedVersion)
		os.Exit(0)
	}

	// Fetch new GTFS Timetable
	slog.Info("Fetching new Timtable GTFS feed", "feedVersion", feedVersion)
	response_timetable, err := client.Get(redirectURL)

	if err != nil {
		slog.Error("Could not download timetable with error", "err", err)
		os.Exit(1)
	}

	// Create temporary directory for unzipped files
	if _, err := os.Stat("tmp"); os.IsNotExist(err) {
		err := os.Mkdir("tmp", 0755)
		if err != nil {
			slog.Error("Could not create temporary directory with error", "err", err)
			os.Exit(1)
		}
	}

	bodyBytes, err := io.ReadAll(response_timetable.Body)

	if err != nil {
		slog.Error("Could not read timetable response body with error", "err", err)
		os.Exit(1)
	}

	slog.Info("Uploading GTFS feed zip to S3", "feedVersion", feedVersion)
	// push full zip to S3
	err = s3.PushS3Object(
		os.Getenv("S3_ENDPOINT"),
		os.Getenv("S3_ACCESS_KEY"),
		os.Getenv("S3_SECRET_KEY"),
		"gtfs-fp",
		zipFileName,
		bodyBytes,
	)

	if err != nil {
		slog.Error("Could not upload GTFS zip to S3 with error", "err", err)
		os.Exit(1)
	}

	reader, err := zip.NewReader(bytes.NewReader(bodyBytes), response_timetable.ContentLength)

	if err != nil {
		slog.Error("Could not create zip reader with error", "err", err)
		os.Exit(1)
	}

	targetFiles := []string{"stops.txt", "routes.txt", "stop_times.txt", "trips.txt"}

	for _, file := range reader.File {

		if !slices.Contains(targetFiles, file.Name) {
			continue
		}

		fileContent, err := file.Open()

		if err != nil {
			slog.Error("Could not open file inside zip with error", "err", err)
			os.Exit(1)
		}

		fileBytes, err := io.ReadAll(fileContent)

		if err != nil {
			os.Exit(1)
		}

		err = os.WriteFile("tmp/"+file.Name, fileBytes, 0644)

		if err != nil {
			slog.Error("Could not write file to temporary directory with error", "err", err)
			os.Exit(1)
		}

		fileContent.Close()
	}

	conn, err := sql.Open("duckdb", dbFileName)

	if err != nil {
		slog.Error("Could not open DuckDB connection with error", "err", err)
		os.Exit(1)
	}

	conn.Exec(`
		CREATE TABLE stops AS SELECT * FROM read_csv('tmp/stops.txt', force_not_null = [location_type, parent_station, platform_code]) WHERE starts_with(stop_id, 'Parent');
	`)

	conn.Exec(`
		CREATE TABLE trips AS SELECT * FROM read_csv('tmp/trips.txt', force_not_null = [block_id, original_trip_id, hints], types={'block_id': 'VARCHAR'});
	`)

	conn.Exec(`
		CREATE TABLE routes AS SELECT * FROM read_csv('tmp/routes.txt', force_not_null = [route_long_name]);
	`)

	conn.Exec(`
		CREATE TABLE stop_times AS SELECT * FROM read_csv('tmp/stop_times.txt');
	`)

	conn.Exec(`
		CREATE INDEX route_id on routes (route_id);
		CREATE INDEX trip_id on trips (trip_id);
		CREATE INDEX stop_id on stops (stop_id);
		CREATE INDEX stop_times_trip_id on stop_times (trip_id);             
	`)

	err = conn.Close()

	if err != nil {
		slog.Error("Could not close DuckDB connection with error", "err", err)
		os.Exit(1)
	}

	err = os.RemoveAll("tmp")
	if err != nil {
		slog.Error("Could not remove temporary directory with error", "err", err)
		os.Exit(1)
	}

	dbBytes, err := os.ReadFile(dbFileName)

	if err != nil {
		slog.Error("Could not read DuckDB file with error", "err", err)
		os.Exit(1)
	}

	err = s3.PushS3Object(
		os.Getenv("S3_ENDPOINT"),
		os.Getenv("S3_ACCESS_KEY"),
		os.Getenv("S3_SECRET_KEY"),
		"gtfs-fp",
		dbFileName,
		dbBytes,
	)

	if err != nil {
		slog.Error("Could not upload DuckDB file to S3 with error", "err", err)
		os.Exit(1)
	}

	slog.Info("Successfully uploaded GTFS feed to S3", "feedVersion", feedVersion)
}
