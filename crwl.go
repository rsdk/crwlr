package main

import (
	"code.google.com/p/go.net/html"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type Site struct {
	Name string
	URL  string
}

/******************
* Bekommt eine komplette HTML Seite
* TODO gibt zusätzlich eine Liste der Links zurück
* Gibt nur den sichtbaren Text einer HTML Seite zurück
*******************/
func parseHtml(r io.Reader) (map[string]int, map[string]int) {
	d := html.NewTokenizer(r)
	var links map[string]int
	var words map[string]int
	links = make(map[string]int)
	words = make(map[string]int)

	for {
		// token type
		tokenType := d.Next()
		if tokenType == html.ErrorToken {
			//fmt.Printf("%v", links)
			//fmt.Printf("%v", words)
			return links, words
		}
		token := d.Token()
		switch tokenType {
		case html.StartTagToken: // <tag>
			// type Token struct {
			//     Type     TokenType
			//     DataAtom atom.Atom
			//     Data     string
			//     Attr     []Attribute
			// }
			//
			// type Attribute struct {
			//     Namespace, Key, Val string
			// }

			//TODO: Eine Map mit Links erstellen
			if token.Data == "a" {
				for _, element := range token.Attr {
					if element.Key == "href" {
						//neuen Link zur Map hinzufügen und hochzählen
						links[element.Val] = links[element.Val] + 1
					}
				}
			}

		case html.TextToken: // text between start and end tag
			//Map mit Wörtern erstellen
			temp := strings.Fields(token.Data)
			for _, element := range temp {
				//TODO: einzelne Örter noch besser von Sonderzeichen trennen
				words[element] = words[element] + 1
			}

			//fmt.Printf("%q\n", temp)
		case html.EndTagToken: // </tag>
		case html.SelfClosingTagToken: // <tag/>

		}
	}
}

/*******************
* Bekommt eine URL
* lädt die Seite herunter
* Gibt die komplette Seite zurück
*******************/
func fetchURL(url string) io.Reader {
	response, err := http.Get(url)
	if err != nil {
		fmt.Printf("Fehler: %s beim HTTP GET von: %s\n", url, err)
	}
	//fmt.Printf("%T", response.Body)
	return response.Body
}

func main() {
	start := time.Now()

	site := Site{"Go", "http://www.heise.de"}

	fmt.Printf("%+v\n", site)
	//fmt.Println(fetchURL(site.URL))
	links, words := parseHtml(fetchURL(site.URL))

	fmt.Printf("%v", links)
	fmt.Println()
	fmt.Println()
	fmt.Printf("%v", words)

	fmt.Println()
	fmt.Printf("Dauer : [%.2fs]\n", time.Since(start).Seconds())
}
