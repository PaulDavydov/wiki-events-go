package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"github.com/opensearch-project/opensearch-go/v4"
	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"
	"github.com/opensearch-project/opensearch-go/v4/opensearchutil"
	"net/http"
	"strings"
)

const IndexName = "go-test-index1"

func (app *application) startOpenSearch() (*opensearchapi.Client, error) {
	// Initiate Opensearch Client
	client, err := opensearchapi.NewClient(
		opensearchapi.Config{
			Client: opensearch.Config{
				Transport: &http.Transport{
					TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
				},
				Addresses: []string{"https://localhost:9200"},
				Username:  "admin",
				Password:  "myStrongPassword123@456",
			},
		},
	)
	if err != nil {
		return client, err
	}

	ctx := context.Background()
	infoResp, err := client.Info(ctx, nil)
	if err != nil {
		return client, err
	}
	fmt.Printf("Cluster INFO:\n Cluster Name: %s\n Cluster UUID: %s\n Version Number: %s\n", infoResp.ClusterName, infoResp.ClusterUUID, infoResp.Version.Number)

	// define index mapping
	mapping := strings.NewReader(`{
		"settings": {
			"index": {
				"number_of_shards": 4
			}
		}
	}`)

	// create an index with non-default settings
	createIndexResponse, err := client.Indices.Create(
		ctx,
		opensearchapi.IndicesCreateReq{
			Index: IndexName,
			Body:  mapping,
		},
	)

	var opensearchError *opensearch.StructError

	if err != nil {
		if errors.As(err, &opensearchError) {
			if opensearchError.Err.Type != "resource_already_exists_exception" {
				return client, err
			}
		} else {
			return client, err
		}
	}

	fmt.Printf("Created Index: %s\n Shards Acknowledged: %t\n", createIndexResponse.Index, createIndexResponse.ShardsAcknowledged)

	return client, nil
}

func (app *application) insertOpenSearchIndex(d document, client opensearchapi.Client) (*opensearchapi.IndexResp, error) {
	insertResp, err := client.Index(
		context.Background(),
		opensearchapi.IndexReq{
			Index: IndexName,
			Body:  opensearchutil.NewJSONReader(&d),
		},
	)
	if err != nil {
		return nil, err
	}

	return insertResp, nil
}
