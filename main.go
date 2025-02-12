package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
)

type Credentials struct {
	accountID   string
	namespaceID string
	keyName     string
	kvToken     string
	duckToken   string
}

var creds Credentials

func main() {
	creds.accountID = os.Getenv("accountID")
	creds.namespaceID = os.Getenv("namespaceID")
	creds.keyName = os.Getenv("keyName")
	creds.kvToken = os.Getenv("kvToken")
	creds.duckToken = os.Getenv("duckToken")

	client := &http.Client{
		Timeout: time.Duration(10 * time.Second),
	}

	duckAddressChan := make(chan struct {
		address string
		err     error
	})

	kvDBChan := make(chan struct {
		db  []byte
		err error
	})

	go func() {
		address, err := getDuckAddress(client)
		duckAddressChan <- struct {
			address string
			err     error
		}{address, err}
	}()

	go func() {
		db, err := getKVdb(client)
		kvDBChan <- struct {
			db  []byte
			err error
		}{db, err}
	}()

	duckResult := <-duckAddressChan
	duckAddress := duckResult.address
	errDuck := duckResult.err

	if errDuck != nil {
		log.Fatalln("Can't create Duck Address", errDuck)
	}

	fmt.Println(duckAddress) // send Duck Addres to Aflred

	kvDBResult := <-kvDBChan
	db := kvDBResult.db
	errDB := kvDBResult.err

	if errDB != nil {
		log.Println("Can't get database", errDB)
		return // if it's impossible to add alias to database, user can still continue with alias
	}

	category := os.Getenv("cf_category")

	db, err := addItemToJSON(db, duckAddress, category)
	if err != nil {
		log.Println("Can't update JSON", err)
	}

	err = setKVdb(client, db)
	if err != nil {
		fmt.Println("Can't update KV database", err)
	}
}

func addItemToJSON(jsonData []byte, newKey string, newValue string) ([]byte, error) {
	var data map[string]string
	err := json.Unmarshal(jsonData, &data)
	if err != nil {
		return nil, fmt.Errorf("Error unmarshaling JSON: %w", err)
	}

	data[newKey] = newValue
	updatedJSON, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("Error marshaling JSON: %w", err)
	}

	return updatedJSON, nil
}

func makeRequest(client *http.Client, method, apiURL string, apiToken string, body io.Reader) ([]byte, error) {

	var req *http.Request
	var err error

	req, err = http.NewRequestWithContext(context.Background(), method, apiURL, body)
	if err != nil {
		return nil, fmt.Errorf("NewRequest failed: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+apiToken)
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("making request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf(
			"request failed with status %d: %s",
			resp.StatusCode, strings.TrimSpace(string(bodyBytes))) //Trim to remove any unnecessary
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading response body: %w", err)
	}

	return data, nil
}

func getDuckAddress(client *http.Client) (string, error) {
	apiURL := "https://quack.duckduckgo.com/api/email/addresses"
	response, err := makeRequest(client, "POST", apiURL, creds.duckToken, nil)
	if err != nil {
		return "", fmt.Errorf("Can't create a Duck request %w", err)
	}

	var data map[string]string
	err = json.Unmarshal(response, &data)
	if err != nil {
		return "", fmt.Errorf("Can't unmarshal Duck JSON: %w", err)
	}
	return (data["address"] + "@duck.com"), nil
}

func getKVdb(client *http.Client) ([]byte, error) {
	apiURL := "https://api.cloudflare.com/client/v4/accounts/" + creds.accountID +
		"/storage/kv/namespaces/" + creds.namespaceID + "/values/" + creds.keyName

	response, err := makeRequest(client, "GET", apiURL, creds.kvToken, nil)
	if err != nil {
		return nil, fmt.Errorf("Can't create a KV request, %w\n", err)
	}

	return response, nil
}

func setKVdb(client *http.Client, db []byte) error {
	apiURL := "https://api.cloudflare.com/client/v4/accounts/" + creds.accountID +
		"/storage/kv/namespaces/" + creds.namespaceID + "/values/" + creds.keyName

	_, err := makeRequest(client, "PUT", apiURL, creds.kvToken, bytes.NewBuffer(db))
	if err != nil {
		return fmt.Errorf("Can't update database: %w", err)
	}
	return nil
}
