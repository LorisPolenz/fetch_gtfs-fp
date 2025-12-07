package url

import (
	"fmt"
	"net/http"
)

func GetRedirectURL(initialURL string) (string, error) {
	client_timeout := &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	resp, err := client_timeout.Get("https://data.opentransportdata.swiss/de/dataset/timetable-2025-gtfs2020/permalink")
	if err != nil {
		// Note: Use proper error handling in production
		panic(err)
	}
	defer resp.Body.Close()

	// 3. Check if the response is a redirect
	// We check against specific redirect status codes (301, 302, 303, 307, 308)
	var redirectURL string

	switch resp.StatusCode {
	case http.StatusMovedPermanently, // 301
		http.StatusFound,             // 302
		http.StatusSeeOther,          // 303
		http.StatusTemporaryRedirect, // 307
		http.StatusPermanentRedirect: // 308

		redirectURL = resp.Header.Get("Location")

	default:
		// Raise exception equivalent
		return "", fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return redirectURL, nil
}
