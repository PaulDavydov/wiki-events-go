package main

import "github.com/r3labs/sse/v2"

func (app *application) startSSE(url string) *sse.Client {
	client := sse.NewClient(url)

	return client
}
