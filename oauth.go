// Utility functions for managing oauth connections.
package main
// Credit to the Go examples, from which this is mostly copied:
// https://code.google.com/p/google-api-go-client/source/browse/examples
// and which are Copyright (c) 2011 Google Inc. All rights reserved.

import (
  "flag"
        "encoding/gob"
        "errors"
        "fmt"
        "hash/fnv"
        "io/ioutil"
        "log"
        "net/http"
        "net/http/httptest"
        "net/url"
        "os"
        "os/exec"
        "path/filepath"
        "runtime"
        "strings"
        "time"

        "code.google.com/p/goauth2/oauth"

)

var clientId     = flag.String("clientid", "", "OAuth Client ID.  If non-empty, overrides --clientid_file")
var clientIdFile = flag.String("clientid_file", "clientid",
                "Name of a file containing just the project's OAuth Client ID from https://console.developers.google.com/project/<project-id>/apiui/credential")
var secret     = flag.String("secret", "", "OAuth Client Secret.  If non-empty, overrides --secret_file")
var secretFile = flag.String("secret_file", "clientsecret",
                "Name of a file containing just the project's OAuth Client Secret from https://console.developers.google.com/project/<project-id>/apiui/credential")
var cacheToken = flag.Bool("cachetoken", true, "cache the OAuth token")
var debug      = flag.Bool("debug", false, "show HTTP traffic")


func osUserCacheDir() string {
        switch runtime.GOOS {
        case "darwin":
                return filepath.Join(os.Getenv("HOME"), "Library", "Caches")
        case "linux", "freebsd":
                return filepath.Join(os.Getenv("HOME"), ".cache")
        }
        log.Printf("TODO: osUserCacheDir on GOOS %q", runtime.GOOS)
        return "."
}

func tokenCacheFile(config *oauth.Config) string {
        hash := fnv.New32a()
        hash.Write([]byte(config.ClientId))
        hash.Write([]byte(config.ClientSecret))
        hash.Write([]byte(config.Scope))
        fn := fmt.Sprintf("fuse-gdrive-token-%v", hash.Sum32())
        return filepath.Join(osUserCacheDir(), url.QueryEscape(fn))
}

func tokenFromFile(file string) (*oauth.Token, error) {
        if !*cacheToken {
                return nil, errors.New("--cachetoken is false")
        }
        f, err := os.Open(file)
        if err != nil {
                return nil, err
        }
        t := new(oauth.Token)
        err = gob.NewDecoder(f).Decode(t)
        return t, err
}

func saveToken(file string, token *oauth.Token) {
        f, err := os.Create(file)
        if err != nil {
                log.Printf("Warning: failed to cache oauth token: %v", err)
                return
        }
        defer f.Close()
        gob.NewEncoder(f).Encode(token)
}

func tokenFromWeb(config *oauth.Config) *oauth.Token {
        ch := make(chan string)
        randState := fmt.Sprintf("st%d", time.Now().UnixNano())
        ts := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
                if req.URL.Path == "/favicon.ico" {
                        http.Error(rw, "", 404)
                        return
                }
                if req.FormValue("state") != randState {
                        log.Printf("State doesn't match: req = %#v", req)
                        http.Error(rw, "", 500)
                        return
                }
                if code := req.FormValue("code"); code != "" {
                        fmt.Fprintf(rw, "<h1>Success</h1>Authorized.")
                        rw.(http.Flusher).Flush()
                        ch <- code
                        return
                }
                log.Printf("no code")
                http.Error(rw, "", 500)
        }))
        defer ts.Close()

        config.RedirectURL = ts.URL
        authUrl := config.AuthCodeURL(randState)
        go openUrl(authUrl)
        log.Printf("Authorize this app at: %s", authUrl)
        code := <-ch
        log.Printf("Got code: %s", code)

        t := &oauth.Transport{
                Config:    config,
                Transport: http.DefaultTransport,
        }
        _, err := t.Exchange(code)
        if err != nil {
                log.Fatalf("Token exchange error: %v", err)
        }
        return t.Token
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

func valueOrFileContents(value string, filename string) string {
        if value != "" {
                return value
        }
        slurp, err := ioutil.ReadFile(filename)
        if err != nil {
                log.Fatalf("Error reading %q: %v", filename, err)
        }
        return strings.TrimSpace(string(slurp))
}

func getOAuthClient(scope string) *http.Client {
        var config = &oauth.Config{
                // Set by --clientid or --clientid_file
                ClientId:     valueOrFileContents(*clientId, *clientIdFile),
                // Set by --secret or --secret_file
                ClientSecret: valueOrFileContents(*secret, *secretFile),
                Scope:        scope, // of access requested (drive, gmail, etc)
                AuthURL:      "https://accounts.google.com/o/oauth2/auth",
                TokenURL:     "https://accounts.google.com/o/oauth2/token",
        }

        cacheFile := tokenCacheFile(config)
        token, err := tokenFromFile(cacheFile)
        if err != nil {
                token = tokenFromWeb(config)
                saveToken(cacheFile, token)
        } else {
                log.Printf("Using cached token %#v from %q", token, cacheFile)
        }

        t := &oauth.Transport{
                Token:     token,
                Config:    config,
                Transport: http.DefaultTransport,
        }
        return t.Client()
}

