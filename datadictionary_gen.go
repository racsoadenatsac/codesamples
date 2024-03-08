package main

import (
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/ayush6624/go-chatgpt"
	"github.com/xuri/excelize/v2"
)

type ChatCompletionResponse struct {
	ID      string `json:"id"`
	Object  string `json:"object"`
	Created int64  `json:"created"`
	Model   string `json:"model"`
	Choices []struct {
		Text          string        `json:"text"`
		Index         int           `json:"index"`
		Logprobs      interface{}   `json:"logprobs"`
		FinishReason  string        `json:"finish_reason"`
		SelectedToken int           `json:"selected_token"`
		GeneratedText string        `json:"generated_text"`
		FullText      string        `json:"full_text"`
		DocTokens     []interface{} `json:"doc_tokens"`
	} `json:"choices"`
}

func main() {

	key := ""
	c, err := chatgpt.NewClient(key)
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()

	file, err := os.Open("Core.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	// Create a new reader.
	reader := csv.NewReader(file)

	var rowsString []string = make([]string, 256)
	// Read all lines.
	rows, err := reader.ReadAll()
	if err != nil {
		log.Fatal(err)
	}
	i := 0
	counter := 0
	for j := range rows {
		counter += 2
		if counter > 32 {
			counter = 0
			i++
		}
		rowsString[i] += strings.Join(rows[j], ".") + ","
	}

	fmt.Println(rowsString)

	//var resString []string

	file2, err := os.OpenFile("", os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer file2.Close()

	for i, item := range rowsString {

		fmt.Println("ITEM: ", item)
		res, err := c.Send(ctx, &chatgpt.ChatCompletionRequest{
			Model: chatgpt.GPT35Turbo,
			Messages: []chatgpt.ChatMessage{
				{
					Role:    chatgpt.ChatGPTModelRoleSystem,
					Content: "Estoy tratando de producir un diccionario de datos para Fineract. Dados los siguientes campos de la base de datos podrías añadir comentarios para cada fila? Por favor se exhaustivo, no resumas, genera todas las filas a partir lo que te doy como entrada. Solo dame una fila por campo en formato CSV, así: tabla.campo,definición. No uses numeros ni tampoco guiones. Solo dame lo que pido, exactamente lo que pido." + rowsString[i],
				},
			},
		})
		if err != nil {
			log.Fatal(err)
		}

		fmt.Fprintf(file2, "%v\n", res)
	}

	/*
			resParts := strings.Split(fmt.Sprintf("%v", res), "{")

			for resLine := range resParts {
				pattern := regexp.MustCompile(`:`)
				input := fmt.Sprintf("%v", resLine)
				x := pattern.Split(input, -1)
				resString = append(resString, x[0])
			}

		//}
		//fmt.Println(resString)
		//fmt.Println(strings.Split(resString[0], ",|"))
		//}
		//fmt.Printf("%#v\n", rowsString[0])
	*/

	/*
	   a, _ = json.MarshalIndent(res, "", "  ");
	   log.Println(string(a))

	   connector, err := dbsql.NewConnector(

	   	dbsql.WithAccessToken(""),
	   	dbsql.WithServerHostname(""),
	   	dbsql.WithPort(),
	   	dbsql.WithHTTPPath(""),

	   )

	   	if err != nil {
	   		panic(err)
	   	}

	   db := sql.OpenDB(connector)
	   defer db.Close()

	   schema := "middleware"

	   dict := openExcelDictionary()
	   dictRows, err := dict.GetRows("Sheet1")

	   	if err != nil {
	   		fmt.Println(err)
	   		os.Exit(1)
	   	}

	   for _, row := range dictRows {

	   	rowData := strings.Join(row, ",")
	   	fmt.Println(rowData)
	   	switch schema {
	   	case "postgres":
	   		alterTable(db, rowData, 0, 1, 2, 3)
	   	case "middleware":
	   		alterTable(db, rowData, 1, 2, 3, 10)
	   	case "core":
	   		alterTable(db, rowData, 0, 1, 2, 3)
	   	default:
	   		alterTable(db, rowData, 0, 1, 2, 3)
	   	}

	   }
	*/
}

func openExcelDictionary() *excelize.File {
	xlsx, err := excelize.OpenFile("")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	return xlsx
}

func alterTable(db *sql.DB, rowData string, pos0, pos1, pos2, pos3 int) {
	catalog := ""
	csvRow := strings.Split(rowData, ",")

	schema := csvRow[pos0]
	table := csvRow[pos1]
	column := csvRow[pos2]
	comment := csvRow[pos3]

	query := "ALTER TABLE " + catalog + "." + schema + "." + table + " ALTER COLUMN " + column + " COMMENT '" + comment + "'"
	fmt.Println(query)
	_, err := db.Exec(query)
	if err != nil {
		fmt.Println(err)
	}
}
