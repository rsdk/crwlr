package main

import (
	"bufio"
	"code.google.com/p/go.net/html" //Tokenizer für HTML
	"database/sql"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

const MaxOutstanding_Fetcher = 100 // Maximale Anzahl gleichzeitger Fetcher
const DEBUG = 1                    // Debugausgabe an/aus
const MaxLinkDepth = 3             // Maximale Linktiefe

type URLINDEX struct {
	URL   string
	WORDS map[string]int
}
type HTTPRESP struct {
	URL       string
	LINKDEPTH int
	FD        io.Reader
}
type URL struct {
	URL       string
	LINKDEPTH int
}

var crwldurls map[string]bool // globale URL Map - um doppeltes HTTP GET zu vermeiden

//Channels
var chan_urls = make(chan URL, 100000) // buffered channel of strings
//bei aktueller implementierung des Parser aufrufs, müsste der io_reader channel nicht buffered sein
var chan_ioreaders = make(chan HTTPRESP, 10)   // buffered channel of structs
var chan_urlindexes = make(chan URLINDEX, 100) // buffered channel of structs

//Semaphore Channel
var sem_Fetcher = make(chan int, MaxOutstanding_Fetcher)

func debugausgabe(msg string) {
	if DEBUG == 1 {
		fmt.Printf("%s DEBUG: %s\n", time.Now(), msg)
	}
}

func handleFetcher(url URL) {
	<-sem_Fetcher // Eine Ressource verbrauchen: Lock falls bereits alle verbraucht
	fetchURL(url)
	sem_Fetcher <- 1 // Eine Ressource wieder freigeben
}

func starten() {
	// Initiales setzen der Ressourcen
	for i := 0; i < MaxOutstanding_Fetcher; i++ {
		sem_Fetcher <- 1
	}
	// Endless (as long as the channel is not empty) Fetcher Spawning
	for {
		go handleFetcher(<-chan_urls)
	}
}

// parseHTML bekommt eine komplette HTML Seite
// und legt eine Map mit Wörtern und (viele) einzelne Links in entsprechende Channcd els
func parseHtml(a HTTPRESP) {
	//start := time.Now()
	d := html.NewTokenizer(a.FD)
	var words map[string]int
	words = make(map[string]int)

	for {
		// token type
		tokenType := d.Next()
		// ErrorToken kommt (auch) beim Ende der Daten
		if tokenType == html.ErrorToken {
			chan_urlindexes <- URLINDEX{a.URL, words} // WORD-Map in den Channel legen
			//fmt.Printf("Parse-Dauer : [%.2fs]  URL: %s\n", time.Since(start).Seconds(), a.URL)
			return
		}
		token := d.Token()
		switch tokenType {
		case html.StartTagToken: // <tag>
			// Links finden
			if token.Data == "a" {
				for _, element := range token.Attr {
					if element.Key == "href" {
						// Link normalisieren
						ref_url, err := url.Parse(element.Val)         // geparste URL
						base_url, _ := url.Parse(a.URL)                // Basis URL der geparsten Seite
						comp_url := base_url.ResolveReference(ref_url) // zusammengesetzte url oder falls ref_url==absoluteurl->ref_url
						// Nur Links die nicht in der globalen Link Map sind
						if err == nil && comp_url.Scheme == "http" && crwldurls[comp_url.String()] != true && a.LINKDEPTH < MaxLinkDepth {
							chan_urls <- URL{comp_url.String(), a.LINKDEPTH + 1} // Die URL in den Channel legen und Linktiefe hochzählen
						}
					}
				}
			}

		case html.TextToken: // text between start and end tag
			//Map mit Wörtern erstellen
			temp := strings.Fields(token.Data) //Aufteilen in Einzelne Wörter, trennen bei Whitespace
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
// und legt ein struct vom Typ HTTPRESP in den Channel chan_ioreaders
func fetchURL(url URL) {
	start := time.Now()
	crwldurls[url.URL] = true //URL in die globale URL Liste aufnehmen damit sie nicht nochmal in den Work Queue kommt.
	response, err := http.Get(url.URL)
	if err != nil {
		fmt.Printf("Fehler: %s beim HTTP GET von: %s\n", err, url.URL)
		return
	}
	//fmt.Printf("%T", response.Body)
	chan_ioreaders <- HTTPRESP{url.URL, url.LINKDEPTH, response.Body}
	fmt.Printf("HTTP GET Dauer: [%.2fs]  URL: %s Tiefe: %d\n", time.Since(start).Seconds(), url.URL, url.LINKDEPTH)
	//TODO io.reader channel umstellen auf direkte übergabe des structs
	go parseHtml(<-chan_ioreaders) // Für jeden Fetcher wird ein Parser aufgerufen, der unabhängig läuft
	return
}

//save holt sich daten (structs vom Typ URLINDEX) aus dem Channel chan_urlindexes
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
		data, ok := <-chan_urlindexes

		if ok == false {
			return // Abbruch wenn der Channel geschlossen ist
		}

		fmt.Fprintf(w, "\nURL: %s\nMAP:\n%v\n\n\n\n", data.URL, data.WORDS)
		w.Flush()
	}
}

//writeDB holt sich daten (structs vom Typ URLINDEX) aus dem Channel chan_urlindexes
// und schreibt die Daten in eine sqlite3 Datenbank: crwld.db
func writeDB() {
	os.Remove("./crwld.db")

	db, err := sql.Open("sqlite3", "./crwld.db")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer db.Close()

	//Neue Tabelle(n) erstellen
	sqls := []string{
		"create table data (url text not null, word text, count int)",
		"delete from data",
	}
	for _, sql := range sqls {
		_, err = db.Exec(sql)
		if err != nil {
			fmt.Printf("%q: %s\n", err, sql)
			return
		}
	}

	// Endlosworker
	for {
		index, ok := <-chan_urlindexes //Neue Arbeit aus dem Channel holen

		if ok == false {
			return // Abbruch wenn der Channel geschlossen ist
		}

		tx, err := db.Begin()
		if err != nil {
			fmt.Println(err)
			return
		}

		stmt, err := tx.Prepare("insert into data(url, word, count) values(?, ?, ?)") //Maske für das SQL Statement setzen
		if err != nil {
			fmt.Println(err)
			return
		}
		defer stmt.Close()

		// Die gesamte MAP durchlaufen und für jeden Key ein SQl Statement zusammensetzen
		for element := range index.WORDS {
			_, err = stmt.Exec(index.URL, element, index.WORDS[element])
			if err != nil {
				fmt.Println(err)
				return
			}
		}
		tx.Commit()
	}
}

func main() {
	// "http://www.rsdk.net/test2/" das letzte / ist wichtig für die Auflösung von relativen URLs
	// aber am besten eine "echte" Startseite wie z.B. index.html angeben.
	starturl := URL{"http://www.rsdk.net/test2/index.html", 0} // Start URL mit Linktiefe 0 festlegen
	chan_urls <- starturl                                      // URL in den Channel legen
	crwldurls = make(map[string]bool)
	//go save() // File Writer starten
	debugausgabe("Starte DB Writer")
	go writeDB() //DB Writer starten
	debugausgabe("Starte Crawler")
	starten() // Crawler starten

}
