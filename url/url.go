package url

import (
	"fmt"
	"log/slog"
	"net/http"
	"os"
)

func GetRedirectURL(initialURL string) (string, error) {
	client := &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	resp, err := client.Get(initialURL)

	if err != nil {
		slog.Error("Could not fetch URL", "err", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	var redirectURL string

	// return the redirect URL if status code indicates a redirect
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
