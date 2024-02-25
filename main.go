package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type WriteFileData struct {
	Data []byte
}

var fileDateChanMap = make(map[string]chan WriteFileData)
var fileDateMap = make(map[string]*os.File)
var bytesWritten int64
var bytesSent int64
var linesProcessed int64

func startFileWriter(name string, c chan WriteFileData) {
	file, err := os.OpenFile(name, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("failed opening file: %s", err)
	}
	for {
		select {
		case data := <-c:
			_, err := file.Write(data.Data)
			if err != nil {
				log.Fatalf("failed writing to file: %s", err)
			}
			atomic.AddInt64(&bytesWritten, int64(len(data.Data)))
		}
	}
}

func processLine(line string, wg *sync.WaitGroup) {
	defer wg.Done()
	atomic.AddInt64(&linesProcessed, 1)
	parts := strings.Split(line, "\t")
	date := strings.Split(parts[3], "T")[0]
	d := []byte(parts[4] + "\n")
	atomic.AddInt64(&bytesSent, int64(len(d)))
	fileDateChanMap[date] <- WriteFileData{Data: d}
}

func generateDates() []string {
	startDate := time.Date(2008, time.January, 1, 0, 0, 0, 0, time.UTC)
	endDate := time.Now().UTC() // Use UTC to avoid timezone issues

	// Create a slice to hold the dates
	var dates []string

	// Loop from start date to end date
	for d := startDate; d.Before(endDate) || d.Equal(endDate); d = d.AddDate(0, 0, 1) {
		// Format the date as "YYYY-MM-DD" and append to the slice
		dates = append(dates, d.Format("2006-01-02"))
	}

	return dates
}

func main() {
	dates := generateDates()
	if len(os.Args) < 3 {
		fmt.Println("Usage: go run book_sort file_path output_dir")
		return
	}

	for i, arg := range os.Args[1:] {
		fmt.Printf("Argument %d: %s\n", i+1, arg)
	}

	filePath := os.Args[1]
	outputDir := os.Args[2]
	//filePath := "/Users/maddox/Desktop/ol_dump_works_2024-01-31.txt"
	//outputDir := "./books_sorted"
	//os.RemoveAll(outputDir)

	now := time.Now()
	file, err := os.Open(filePath)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	os.Mkdir(outputDir, 0755)

	scanner := bufio.NewScanner(file)
	bufferSize := 1024 * 1024 // 1MB
	buffer := make([]byte, bufferSize)
	scanner.Buffer(buffer, bufferSize)

	var wg sync.WaitGroup
	semaphore := make(chan struct{}, 150) // Limit to 50 goroutines.

	count := 0

	println("starting process")
	for _, d := range dates {
		fileDateChanMap[d] = make(chan WriteFileData, 500)
		go startFileWriter(outputDir+"/"+d+".txt", fileDateChanMap[d])
	}

	for scanner.Scan() {
		count++
		if count%1_000_000 == 0 {
			log.Printf("processed %d lines", count)
		}
		wg.Add(1)
		semaphore <- struct{}{} // Acquire semaphore
		go func(line string) {
			defer func() { <-semaphore }() // Release semaphore
			processLine(line, &wg)
		}(scanner.Text())
	}

	if err := scanner.Err(); err != nil {
		log.Fatalf("error reading file: %s", err)
	}

	wg.Wait() // Wait for all goroutines to finish

	took := time.Since(now)
	log.Printf("took %s", took)
	totalWritten := atomic.LoadInt64(&bytesWritten)
	log.Printf("total mb written: %d", totalWritten/1024/1024)
	totalSent := atomic.LoadInt64(&bytesSent)
	log.Printf("total mb sent: %d", totalSent/1024/1024)
	log.Printf("lines processed: %d", atomic.LoadInt64(&linesProcessed))

	for _, file := range fileDateMap {
		_ = file.Close()
	}
}
