package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"time"
)

type Site struct {
	Name string
	URL  string
}

func fetchURL(url string) string {
	response, err := http.Get(url)
	if err != nil {
		fmt.Printf("Fehler: %s beim HTTP GET von: %s\n", url, err)
	}
	defer response.Body.Close()
	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		fmt.Printf("Fehler beim Lesen der Daten: %v\n", err)
	}
	return string(body)
}

func main() {
	start := time.Now()

	site := Site{"Go", "http://golang.org"}

	fmt.Printf("%+v\n", site)
	fmt.Println(fetchURL(site.URL))

	fmt.Println()
	fmt.Printf("Dauer : [%.2fs]\n", time.Since(start).Seconds())
}
