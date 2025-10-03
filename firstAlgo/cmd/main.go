package main

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"fmt"
	"log"
	"math/rand"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"time"
)

const (
	keySize    = 99999
	charSet    = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$%^&*()_+-={}|;:,.<>?"
	fileALines = 99999
	wordSize   = 20
	separator  = keySize + 1
)

type FileData struct {
	Key  int
	Word string
	Date string
}

func generateRandomWord(charSet string, size int) string {
	var builder strings.Builder
	for range size {
		builder.WriteByte(charSet[rand.Intn(len(charSet))])
	}
	return builder.String()
}

func generateRandomDate() string {
	month := rand.Intn(11) + 1
	var day int
	switch month {
	case 4, 6, 9, 11:
		day = rand.Intn(29) + 1
	case 2:
		day = rand.Intn(27) + 1
	default:
		day = rand.Intn(30) + 1
	}
	year := rand.Intn(2024) + 1
	set := fmt.Sprintf("%02d/%02d/%04d", day, month, year)
	return set
}

func generateRandomLine() string {
	return fmt.Sprintf("%d\t%s\t%s", rand.Intn(keySize),
		generateRandomWord(charSet, wordSize), generateRandomDate())
}

func parseRandomLine(line string) (FileData, error) {
	r := csv.NewReader(strings.NewReader(line))
	r.Comma = '\t'
	fields, err := r.Read()
	if err != nil {
		return FileData{}, fmt.Errorf("csv read error: %w", err)
	}

	if len(fields) < 3 {
		return FileData{}, fmt.Errorf("not enough fields")
	}

	key, err := strconv.Atoi(fields[0])
	if err != nil {
		return FileData{}, fmt.Errorf("invalid key: %w", err)
	}

	return FileData{
		Key:  key,
		Word: fields[1],
		Date: fields[2],
	}, nil
}

func generateRandomFileA() {
	var content bytes.Buffer
	for range fileALines {
		content.WriteString(generateRandomLine())
		content.WriteByte('\n')
	}
	err := os.WriteFile("A.txt", content.Bytes(), 0644)
	if err != nil {
		log.Fatal(err)
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

	outC, err := os.Create(fileC)
	if err != nil {
		return false, fmt.Errorf("failed to create %s: %w", fileC, err)
	}
	defer func(outC *os.File) {
		if err := outC.Close(); err != nil {
			log.Printf("failed to close outC: %w", err)
		}
	}(outC)

	scanner := bufio.NewScanner(fileA)
	prevKey := 0
	currOutput := outB
	sorted := true

	for scanner.Scan() {
		line := scanner.Text()
		data, err := parseRandomLine(line)
		if err != nil {
			return false, fmt.Errorf("failed to parse line: %w", err)
		}

		if data.Key < prevKey {
			if currOutput == outB {
				_, _ = currOutput.WriteString(fmt.Sprintf("%d\t%s\t%s\n", keySize+1, data.Word, data.Date))
				currOutput = outC
			} else {
				_, _ = currOutput.WriteString(fmt.Sprintf("%d\t%s\t%s\n", keySize+1, data.Word, data.Date))
				currOutput = outB
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

	scannerB := bufio.NewScanner(inB)
	scannerC := bufio.NewScanner(inC)

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
				_, err := out.WriteString(lineC + "\n")
				if err != nil {
					return err
				}
				hasC = scannerC.Scan()
				continue
			}
			if flagC {
				_, err := out.WriteString(lineB + "\n")
				if err != nil {
					return err
				}
				hasB = scannerB.Scan()
				continue
			}
			if dataB.Key <= dataC.Key {
				_, err := out.WriteString(lineB + "\n")
				if err != nil {
					return err
				}
				hasB = scannerB.Scan()
			} else {
				_, err := out.WriteString(lineC + "\n")
				if err != nil {
					return err
				}
				hasC = scannerC.Scan()
			}
		} else if hasB {
			if dataB.Key == separator {
				break
			}
			_, err := out.WriteString(lineB + "\n")
			if err != nil {
				return err
			}
			hasB = scannerB.Scan()
		} else if hasC {
			if dataC.Key == separator {
				break
			}
			_, err := out.WriteString(lineC + "\n")
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
	//generateRandomFileA()
	err := sortFile("A.txt")
	if err != nil {
		return
	}
	//time.Sleep(2 * time.Second)
	fmt.Println(currtime.Sub(time.Now()).Seconds() * -1)
}
