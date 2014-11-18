package main

import (
  "code.google.com/p/google-api-go-client/drive/v2"
  "fmt"
)

// AllFiles fetches and returns all files
// TODO(asjoyner): optimize later, request only what we end up using:
// https://developers.google.com/gdata/docs/2.0/basics#PartialResponse
func AllFiles(d *drive.Service) ([]*drive.File, error) {
  var fs []*drive.File
  pageToken := ""
  for {
    q := d.Files.List()
    // If we have a pageToken set, apply it to the query
    if pageToken != "" {
      q = q.PageToken(pageToken)
    }
    r, err := q.Do()
    if err != nil {
      fmt.Printf("An error occurred: %v\n", err)
      return fs, err
    }
    fs = append(fs, r.Items...)
    pageToken = r.NextPageToken
    if pageToken == "" {
      break
    }
  }
  return fs, nil
}
