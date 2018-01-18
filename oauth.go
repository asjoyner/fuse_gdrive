package main

// oauth.go contains utility functions for managing oauth connections.

// Credit to the Go examples, from which this is mostly copied:
// https://developers.google.com/drive/web/quickstart/go
// and which are Copyright (c) 2015 Google Inc. All rights reserved.

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"

	"golang.org/x/net/context"
	"golang.org/x/oauth2"
)

const (
	defaultClientId string = "902751591868-ghc6jn2vquj6s8n5v5np2i66h3dh5pqq.apps.googleusercontent.com"
	defaultSecret   string = "LLsUuv2NoLglNKx14t5dA9SC"
)

var (
	clientID     = flag.String("clientid", defaultClientId, "OAuth Client ID")
	clientSecret = flag.String("secret", defaultSecret, "OAuth Client Secret")
	cacheToken   = flag.Bool("cachetoken", true, "cache the OAuth token")
	httpDebug    = flag.Bool("http.debug", false, "show HTTP traffic")
)

// getClient uses a Context and Config to retrieve a Token
// then generate a Client. It returns the generated Client.
func getClient(ctx context.Context, scope ...string) *http.Client {
	config := &oauth2.Config{
		ClientID:     *clientID,
		ClientSecret: *clientSecret,
		RedirectURL:  fmt.Sprintf("http://localhost:%s/auth", *port),
		Scopes:       scope,
		Endpoint: oauth2.Endpoint{
			AuthURL:  "https://accounts.google.com/o/oauth2/auth",
			TokenURL: "https://accounts.google.com/o/oauth2/token",
		},
	}
	cacheFile, err := tokenCacheFile()
	if err != nil {
		log.Fatalf("Unable to get path to cached credential file. %v", err)
	}
	tok, err := tokenFromFile(cacheFile)
	if err != nil {
		tok = getTokenFromWeb(config)
		saveToken(cacheFile, tok)
	}
	return config.Client(ctx, tok)
}

func openUrl(url string) {
	try := []string{"xdg-open", "google-chrome", "open"}
	for _, bin := range try {
		err := exec.Command(bin, url).Run()
		if err == nil {
			return
		}
	}
	log.Printf("Error opening URL in browser.")
}

// getTokenFromWeb uses Config to request a Token.
// It returns the retrieved Token.
func getTokenFromWeb(config *oauth2.Config) *oauth2.Token {
	ch := make(chan string)
	http.HandleFunc("/auth", func(rw http.ResponseWriter, req *http.Request) {
		if code := req.FormValue("code"); code != "" {
			fmt.Fprintf(rw, "<h1>Success</h1>Authorized.")
			rw.(http.Flusher).Flush()
			ch <- code
			return
		}
		log.Printf("no code")
		http.Error(rw, "", 500)
	})

	authURL := config.AuthCodeURL("state-token", oauth2.AccessTypeOffline)

	go openUrl(authURL)
	log.Printf("Authorize this app at: %s", authURL)

	code := <-ch
	log.Printf("received code")

	tok, err := config.Exchange(oauth2.NoContext, code)
	if err != nil {
		log.Fatalf("Unable to retrieve token from web %v", err)
	}
	return tok
}

// tokenCacheFile generates credential file path/filename.
// It returns the generated credential path/filename.
func tokenCacheFile() (string, error) {
	tokenCacheDir := filepath.Join(osDataDir(), ".credentials")
	os.MkdirAll(tokenCacheDir, 0700)
	return filepath.Join(tokenCacheDir, "fuse-gdrive-token.json"), nil
}

// tokenFromFile retrieves a Token from a given file path.
// It returns the retrieved Token and any read error encountered.
func tokenFromFile(file string) (*oauth2.Token, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	t := &oauth2.Token{}
	err = json.NewDecoder(f).Decode(t)
	defer f.Close()
	return t, err
}

// saveToken uses a file path to create a file and store the
// token in it.
func saveToken(file string, token *oauth2.Token) {
	fmt.Printf("Saving credential file to: %s\n", file)
	f, err := os.Create(file)
	if err != nil {
		log.Fatalf("Unable to cache oauth token: %v", err)
	}
	defer f.Close()
	json.NewEncoder(f).Encode(token)
	log.Printf("saved token to %s", file)
}
