package main

import (
	"bufio"
	"code.google.com/p/go.net/html" //Tokenizer für HTML
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

type URLINDEX struct {
	URL   string
	WORDS map[string]int
}

type HTTPRESP struct {
	URL string
	FD  io.Reader
}

const MaxOutstanding_URL = 100
const MaxOutstanding_RESP = 4
const DEBUG = 1

//Channels
var chan_urls = make(chan string, 100000)      // buffered channel of strings
var chan_ioreaders = make(chan HTTPRESP, 200)  // buffered channel of structs
var chan_urlindexes = make(chan URLINDEX, 100) // buffered channel of structs
//Semaphore Channels
var sem_URL = make(chan int, MaxOutstanding_URL)
var sem_RESP = make(chan int, MaxOutstanding_RESP)

func debugausgabe(msg string) {
	if DEBUG == 1 {

		fmt.Printf("\n%s DEBUG: %s\n", time.Now(), msg)
	}
}

func handleFetcher(url string) {
	//debugausgabe("Fetcher starten")
	<-sem_URL     // Wait for active queue to drain.
	fetchURL(url) // May take a long time.
	sem_URL <- 1  // Done; enable next request to run.
}

func handleParser(a HTTPRESP) {
	//debugausgabe("Parser starten")
	<-sem_RESP    // Wait for active queue to drain.
	parseHtml(a)  // May take a long time.
	sem_RESP <- 1 // Done; enable next request to run.
}

func starten() {
	//Initiales setzen der Ressourcen
	for i := 0; i < MaxOutstanding_URL; i++ {
		sem_URL <- 1
	}
	//Initiales setzen der Ressourcen
	for i := 0; i < MaxOutstanding_RESP; i++ {
		sem_RESP <- 1
	}
	for {
		work := <-chan_urls
		go handleFetcher(work) // Don't wait for handle to finish.
		work2 := <-chan_ioreaders
		go handleParser(work2)
	}
}

// parseHTML bekommt eine komplette HTML Seite
// und gibt je eine Map mit Links und Wörtern zurück
func parseHtml(a HTTPRESP) {
	start := time.Now()
	d := html.NewTokenizer(a.FD)
	var links map[string]int
	var words map[string]int
	links = make(map[string]int)
	words = make(map[string]int)

	for {
		// token type
		tokenType := d.Next()
		if d.Err() != nil {
			fmt.Printf("TokenERR vorher: %v", d.Err())
		}

		if tokenType == html.ErrorToken {
			chan_urlindexes <- URLINDEX{a.URL, words}
			fmt.Printf("Parse-Dauer : [%.2fs]  URL: %s\n", time.Since(start).Seconds(), a.URL)
			return
		}
		token := d.Token()
		switch tokenType {
		case html.StartTagToken: // <tag>
			//Eine Map mit Links erstellen
			if token.Data == "a" {
				for _, element := range token.Attr {
					if element.Key == "href" {
						//Link normalisieren
						url, err := url.Parse(element.Val)
						//debugausgabe(url.Scheme)
						if url.IsAbs() && err == nil && url.Scheme == "http" {
							//neuen Link zur Map hinzufügen und hochzählen
							links[element.Val] = links[element.Val] + 1
							if links[element.Val] == 1 {
								chan_urls <- element.Val //URL CHANNEL füllen
							}

						}
					}
				}
			}

		case html.TextToken: // text between start and end tag
			//Map mit Wörtern erstellen
			temp := strings.Fields(token.Data)
			for _, element := range temp {
				//TODO: einzelne Örter noch besser von Sonderzeichen trennen z.b. mit TRIM()
				words[element] = words[element] + 1
			}

			//fmt.Printf("%q\n", temp)
		case html.EndTagToken: // </tag>
		case html.SelfClosingTagToken: // <tag/>

		}
	}
}

// fetchURL Bekommt eine URL
// lädt die Seite herunter
// Gibt die komplette Seite zurück
func fetchURL(url string) {
	//start := time.Now()
	response, err := http.Get(url)
	if err != nil {
		fmt.Printf("Fehler: %s beim HTTP GET von: %s\n", err, url)
		return
	}
	//fmt.Printf("%T", response.Body)
	chan_ioreaders <- HTTPRESP{url, response.Body}
	//fmt.Printf("Dauer : [%.2fs]  URL: %s\n", time.Since(start).Seconds(), url)
	return
}

//save holt sich daten (structs vom Typ urlindexes) aus dem Channel chan_urlindexes
// und schreibt die Daten in eine Datei "output.txt"
func save() {

	//fo, err := os.OpenFile("output.txt", os.O_APPEND, 0777)
	fo, err := os.Create("output.txt")
	if err != nil {
		panic(err)
	}
	// close fo on exit and check for its returned error
	defer func() {
		if err := fo.Close(); err != nil {
			panic(err)
		}
	}()
	// make a write buffer
	w := bufio.NewWriter(fo)
	for {
		data := <-chan_urlindexes
		fmt.Fprintf(w, "\nURL: %s\nMAP:\n%v\n\n\n\n", data.URL, data.WORDS)
		w.Flush()
	}
}

func main() {

	chan_urls <- "http://www.ebay.com"

	go save()
	starten()

	//TODO paralleles schreiben in db
}
