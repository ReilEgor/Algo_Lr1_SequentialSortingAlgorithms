/*
Напиши програму мовою Go, яка сортує за зростанням великий текстовий файл розміром до 1 Гб і більше.
Кожен рядок файлу має формати запису: ключ\t20символів\tдата\n. Сортування виконується тільки
за ключем який має тип int, решта має тип string і розглядається як дані. Обмеження на використання пам'яті - 300мб.
Сортування повинно виконуватись за 2-5 хвилини.
*/
package main

import (
	"bufio"
	"container/heap"
	"fmt"
	"os"
	//"path/filepath"
	"sort"
	"strconv"
	"strings"
)

type record struct {
	key  int
	data string
}

// ---------------- Min-Heap для злиття ----------------
type fileRecord struct {
	rec  record
	file int // індекс файлу
}

type minHeap []fileRecord

func (h minHeap) Len() int            { return len(h) }
func (h minHeap) Less(i, j int) bool  { return h[i].rec.key < h[j].rec.key }
func (h minHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *minHeap) Push(x interface{}) { *h = append(*h, x.(fileRecord)) }
func (h *minHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// ------------------------------------------------------

func parseLine(line string) (record, error) {
	parts := strings.SplitN(line, "\t", 2)
	if len(parts) < 2 {
		return record{}, fmt.Errorf("bad line: %s", line)
	}
	key, err := strconv.Atoi(parts[0])
	if err != nil {
		return record{}, err
	}
	return record{key: key, data: parts[1]}, nil
}

func main() {

	inputFile := "A.txt"
	outputFile := "A`.txt"

	// ---- Етап 1: Розбиття на відсортовані чанки ----
	const maxLinesInChunk = 2_000_000 // приблизно під ліміт пам'яті
	var tempFiles []string

	f, err := os.Open(inputFile)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	var chunk []record
	chunkIdx := 0

	for scanner.Scan() {
		rec, err := parseLine(scanner.Text())
		if err != nil {
			continue
		}
		chunk = append(chunk, rec)
		if len(chunk) >= maxLinesInChunk {
			tmpName := fmt.Sprintf("chunk_%d.tmp", chunkIdx)
			writeChunk(tmpName, chunk)
			tempFiles = append(tempFiles, tmpName)
			chunk = nil
			chunkIdx++
		}
	}
	if len(chunk) > 0 {
		tmpName := fmt.Sprintf("chunk_%d.tmp", chunkIdx)
		writeChunk(tmpName, chunk)
		tempFiles = append(tempFiles, tmpName)
	}

	// ---- Етап 2: K-way merge ----
	mergeFiles(tempFiles, outputFile)

	// очищення
	for _, t := range tempFiles {
		os.Remove(t)
	}
}

func writeChunk(filename string, records []record) {
	sort.Slice(records, func(i, j int) bool {
		return records[i].key < records[j].key
	})
	f, err := os.Create(filename)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	for _, r := range records {
		fmt.Fprintf(w, "%d\t%s\n", r.key, r.data)
	}
	w.Flush()
}

func mergeFiles(tempFiles []string, out string) {
	outF, err := os.Create(out)
	if err != nil {
		panic(err)
	}
	defer outF.Close()
	writer := bufio.NewWriter(outF)

	// відкриваємо всі тимчасові файли
	readers := make([]*bufio.Scanner, len(tempFiles))
	files := make([]*os.File, len(tempFiles))
	for i, fname := range tempFiles {
		f, err := os.Open(fname)
		if err != nil {
			panic(err)
		}
		files[i] = f
		readers[i] = bufio.NewScanner(f)
	}

	h := &minHeap{}
	heap.Init(h)

	// читаємо перші рядки
	for i, sc := range readers {
		if sc.Scan() {
			rec, _ := parseLine(sc.Text())
			heap.Push(h, fileRecord{rec: rec, file: i})
		}
	}

	for h.Len() > 0 {
		fr := heap.Pop(h).(fileRecord)
		fmt.Fprintf(writer, "%d\t%s\n", fr.rec.key, fr.rec.data)

		if readers[fr.file].Scan() {
			rec, _ := parseLine(readers[fr.file].Text())
			heap.Push(h, fileRecord{rec: rec, file: fr.file})
		}
	}

	writer.Flush()
	for _, f := range files {
		f.Close()
	}
}
