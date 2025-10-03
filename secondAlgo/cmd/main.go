package main

import (
	"bufio"
	"fmt"
	"log"
	"math/rand"
	"os"
	"runtime"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	keySize    = 300000
	charSet    = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$%^&*()_+-={}|;:,.<>?"
	fileALines = 300000
	wordSize   = 20
	separator  = -1
	bufferSize = 64 * 1024 * 1024
)

type FileData struct {
	Key  int
	Word string
	Date string
}

func generateRandomWord(charSet string, size int) string {
	var builder strings.Builder
	builder.Grow(size)
	for i := 0; i < size; i++ {
		builder.WriteByte(charSet[rand.Intn(len(charSet))])
	}
	return builder.String()
}

func generateRandomDate() string {
	year := rand.Intn(2024-1970+1) + 1970
	month := rand.Intn(12) + 1
	var maxDay int
	switch month {
	case 4, 6, 9, 11:
		maxDay = 30
	case 2:
		maxDay = 28
	default:
		maxDay = 31
	}
	day := rand.Intn(maxDay) + 1
	return fmt.Sprintf("%02d/%02d/%04d", day, month, year)
}

func generateRandomLine() string {
	return fmt.Sprintf("%d\t%s\t%s", rand.Intn(keySize),
		generateRandomWord(charSet, wordSize), generateRandomDate())
}

func parseRandomLine(line string) (FileData, error) {
	parts := strings.Split(line, "\t")
	if len(parts) != 3 {
		return FileData{}, fmt.Errorf("invalid line format")
	}
	key, err := strconv.Atoi(parts[0])
	if err != nil {
		return FileData{}, err
	}
	return FileData{
		Key:  key,
		Word: parts[1],
		Date: parts[2],
	}, nil
}

func generateRandomFileA() {
	file, err := os.Create("A.txt")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	writer := bufio.NewWriterSize(file, bufferSize)
	defer writer.Flush()

	for i := 0; i < fileALines; i++ {
		_, err := writer.WriteString(generateRandomLine() + "\n")
		if err != nil {
			log.Fatal(err)
		}
	}
}

func cleanupTempFiles(files ...string) {
	for _, file := range files {
		if _, err := os.Stat(file); err == nil {
			os.Remove(file)
		}
	}
}
func distributeRuns(sourceFile, fileB, fileC string) (bool, error) {
	fileA, err := os.Open(sourceFile)
	if err != nil {
		return false, fmt.Errorf("failed to open %s: %w", sourceFile, err)
	}
	defer func(fileA *os.File) {
		if err := fileA.Close(); err != nil {
			log.Printf("failed to close fileA: %w", err)
		}
	}(fileA)

	outB, err := os.Create(fileB)
	if err != nil {
		return false, fmt.Errorf("failed to create %s: %w", fileB, err)
	}
	defer func(outB *os.File) {
		if err := outB.Close(); err != nil {
			log.Printf("failed to close outB: %w", err)
		}
	}(outB)
	writerB := bufio.NewWriterSize(outB, 32*1024*1024)
	defer writerB.Flush()
	outC, err := os.Create(fileC)
	if err != nil {
		return false, fmt.Errorf("failed to create %s: %w", fileC, err)
	}
	defer func(outC *os.File) {
		if err := outC.Close(); err != nil {
			log.Printf("failed to close outC: %w", err)
		}
	}(outC)
	writerC := bufio.NewWriterSize(outC, 32*1024*1024)
	defer writerC.Flush()

	scanner := bufio.NewScanner(fileA)
	buf := make([]byte, 0, 128*1024*1024)
	scanner.Buffer(buf, 128*1024*1024)
	prevKey := 0
	currOutput := writerB
	sorted := true

	for scanner.Scan() {
		line := scanner.Text()
		data, err := parseRandomLine(line)
		if err != nil {
			return false, fmt.Errorf("failed to parse line: %w", err)
		}

		if data.Key < prevKey {
			if currOutput == writerB {
				_, _ = currOutput.WriteString(fmt.Sprintf("%d\t%s\t%s\n", separator, data.Word, data.Date))
				currOutput = writerC
			} else {
				_, _ = currOutput.WriteString(fmt.Sprintf("%d\t%s\t%s\n", separator, data.Word, data.Date))
				currOutput = writerB
			}
			sorted = false
		}

		if _, err := currOutput.WriteString(line + "\n"); err != nil {
			return false, fmt.Errorf("failed to write to temp file: %w", err)
		}

		prevKey = data.Key
	}
	return sorted, nil
}

func firstDistributeRuns(sourceFile, fileB, fileC string) error {
	fileA, err := os.Open(sourceFile)
	if err != nil {
		return fmt.Errorf("failed to open %s: %w", sourceFile, err)
	}
	defer fileA.Close()

	outB, err := os.Create(fileB)
	if err != nil {
		return fmt.Errorf("failed to create %s: %w", fileB, err)
	}
	defer outB.Close()

	outC, err := os.Create(fileC)
	if err != nil {
		return fmt.Errorf("failed to create %s: %w", fileC, err)
	}
	defer outC.Close()

	scanner := bufio.NewScanner(fileA)
	currOutput := outB
	var dataLines []FileData
	const blockSize = 1000000

	for scanner.Scan() {
		line := scanner.Text()
		parsedLine, err := parseRandomLine(line)
		if err != nil {
			return fmt.Errorf("failed to parse line: %w", err)
		}
		dataLines = append(dataLines, parsedLine)

		if len(dataLines) >= blockSize {
			sort.Slice(dataLines, func(i, j int) bool {
				return dataLines[i].Key < dataLines[j].Key
			})

			for _, d := range dataLines {
				fmt.Fprintf(currOutput, "%d\t%s\t%s\n", d.Key, d.Word, d.Date)
			}
			fmt.Fprintf(currOutput, "%d\t-1\t-1\n", separator)

			if currOutput == outB {
				currOutput = outC
			} else {
				currOutput = outB
			}
			dataLines = dataLines[:0]
		}
	}

	if len(dataLines) > 0 {
		sort.Slice(dataLines, func(i, j int) bool {
			return dataLines[i].Key < dataLines[j].Key
		})
		for _, d := range dataLines {
			fmt.Fprintf(currOutput, "%d\t%s\t%s\n", d.Key, d.Word, d.Date)
		}
	}

	return scanner.Err()
}

func mergeFiles(destFile, fileB, fileC string) error {
	inB, err := os.Open(fileB)
	if err != nil {
		return fmt.Errorf("failed to open %s: %w", fileB, err)
	}
	defer func(inB *os.File) {
		err := inB.Close()
		if err != nil {
			log.Printf("failed to close inB: %w", err)
		}
	}(inB)

	inC, err := os.Open(fileC)
	if err != nil {
		return fmt.Errorf("failed to open %s: %w", fileC, err)
	}
	defer func(inC *os.File) {
		err := inC.Close()
		if err != nil {
			log.Printf("failed to close inC: %w", err)
		}
	}(inC)

	out, err := os.Create(destFile)
	if err != nil {
		return fmt.Errorf("failed to create %s: %w", destFile, err)
	}
	defer func(out *os.File) {
		err := out.Close()
		if err != nil {
			log.Printf("failed to close out: %w", err)
		}
	}(out)
	writer := bufio.NewWriterSize(out, bufferSize)
	defer writer.Flush()

	scannerB := bufio.NewScanner(inB)
	bufB := make([]byte, 0, 64*1024*1024)
	scannerB.Buffer(bufB, 64*1024*1024)

	scannerC := bufio.NewScanner(inC)
	bufC := make([]byte, 64*1024*1024)
	scannerC.Buffer(bufC, 64*1024*1024)

	hasB := scannerB.Scan()
	hasC := scannerC.Scan()

	var dataB, dataC FileData
	var lineB, lineC string

	flagB := false
	flagC := false

	for hasB || hasC {
		if hasB {
			lineB = scannerB.Text()
			dataB, err = parseRandomLine(lineB)
			if err != nil {
				return fmt.Errorf("failed to parse line: %w", err)
			}
			if dataB.Key == separator {
				flagB = true
			}
		}
		if hasC {
			lineC = scannerC.Text()
			dataC, err = parseRandomLine(lineC)
			if err != nil {
				return fmt.Errorf("failed to parse line: %w", err)
			}
			if dataC.Key == separator {
				flagC = true
			}
		}

		if hasB && hasC {
			if flagB && flagC {
				flagB = false
				flagC = false
				hasB = scannerB.Scan()
				hasC = scannerC.Scan()
				continue
			}
			if flagB {
				_, err := writer.WriteString(lineC + "\n")
				if err != nil {
					return err
				}
				hasC = scannerC.Scan()
				continue
			}
			if flagC {
				_, err := writer.WriteString(lineB + "\n")
				if err != nil {
					return err
				}
				hasB = scannerB.Scan()
				continue
			}
			if dataB.Key <= dataC.Key {
				_, err := writer.WriteString(lineB + "\n")
				if err != nil {
					return err
				}
				hasB = scannerB.Scan()
			} else {
				_, err := writer.WriteString(lineC + "\n")
				if err != nil {
					return err
				}
				hasC = scannerC.Scan()
			}
		} else if hasB {
			if dataB.Key == separator {
				break
			}
			_, err := writer.WriteString(lineB + "\n")
			if err != nil {
				return err
			}
			hasB = scannerB.Scan()
		} else if hasC {
			if dataC.Key == separator {
				break
			}
			_, err := writer.WriteString(lineC + "\n")
			if err != nil {
				return err
			}
			hasC = scannerC.Scan()
		}
	}
	return nil
}
func sortFile(filePath string) error {
	tempFileB := "B.txt"
	tempFileC := "C.txt"

	defer cleanupTempFiles(tempFileB, tempFileC)
	firstDistributeRuns(filePath, tempFileB, tempFileC)
	if err := mergeFiles(filePath, tempFileB, tempFileC); err != nil {
		return fmt.Errorf("failed to merge files: %w", err)
	}
	for {
		sorted, err := distributeRuns(filePath, tempFileB, tempFileC)
		if err != nil {
			return fmt.Errorf("failed to distribute runs: %w", err)
		}

		if sorted {
			return nil
		}

		if err := mergeFiles(filePath, tempFileB, tempFileC); err != nil {
			return fmt.Errorf("failed to merge files: %w", err)
		}
	}
}

func monitorMemory() {
	go func() {
		for {
			var m runtime.MemStats
			runtime.ReadMemStats(&m)

			currentMB := m.Alloc / 1024 / 1024
			if currentMB > 300 {
				fmt.Printf("EXCEEDED LIMIT: %d MB > 300 MB\n", currentMB)
			} else {
				fmt.Printf("Memory is normal: %d MB / 300 MB\n", currentMB)
			}

			time.Sleep(500 * time.Millisecond)
		}
	}()
}

func main() {
	currtime := time.Now()
	debug.SetMemoryLimit(300 * 1024 * 1024)
	monitorMemory()
	rand.Seed(time.Now().UnixNano())
	generateRandomFileA()
	//source := "A.txt"
	//
	//if err := sortFile(source); err != nil {
	//	log.Fatalf("external sort failed: %v", err)
	//}
	fmt.Printf("Sorting completed in %.2f seconds\n", time.Since(currtime).Seconds())
}
