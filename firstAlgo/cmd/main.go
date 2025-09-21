package main

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
)

const (
	keySize    = 12
	charSet    = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$%^&*()_+-={}|;:,.<>?"
	fileALines = 12
	wordSize   = 20
)

type FileData struct {
	Key  int
	Word string
	Date string
}

func generateRandomWord(charSet string, size int) string {
	set := ""
	for range size {
		set += string(charSet[rand.Intn(len(charSet))])
	}
	return set
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
	return fmt.Sprintf("%d\t%s\t%s", (rand.Intn(keySize)),
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
	defer fileA.Close()

	outB, err := os.Create(fileB)
	if err != nil {
		return false, fmt.Errorf("failed to create %s: %w", fileB, err)
	}
	defer outB.Close()

	outC, err := os.Create(fileC)
	if err != nil {
		return false, fmt.Errorf("failed to create %s: %w", fileC, err)
	}
	defer outC.Close()

	scanner := bufio.NewScanner(fileA)
	prevKey := 0
	currOutput := outB
	sorted := true

	for scanner.Scan() {
		line := scanner.Text()
		data, err := parseRandomLine(line)
		if err != nil {
			return false, fmt.Errorf("Failed to parse line: %w", err)
		}

		if data.Key < prevKey {
			if currOutput == outB {
				_, _ = currOutput.WriteString(fmt.Sprintf("%d\t%s\t%s", keySize+1, data.Word, data.Date) + "\n")
				currOutput = outC
			} else {
				_, _ = currOutput.WriteString(fmt.Sprintf("%d\t%s\t%s", keySize+1, data.Word, data.Date) + "\n")
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
	defer inB.Close()

	inC, err := os.Open(fileC)
	if err != nil {
		return fmt.Errorf("failed to open %s: %w", fileC, err)
	}
	defer inC.Close()

	out, err := os.Create(destFile)
	if err != nil {
		return fmt.Errorf("failed to create %s: %w", destFile, err)
	}
	defer out.Close()

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
			dataB, _ = parseRandomLine(lineB)
			if dataB.Key == keySize+1 {
				flagB = true
			}
		}
		if hasC {
			lineC = scannerC.Text()
			dataC, _ = parseRandomLine(lineC)
			if dataC.Key == keySize+1 {
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
				out.WriteString(lineC + "\n")
				hasC = scannerC.Scan()
				continue
			}
			if flagC {
				out.WriteString(lineB + "\n")
				hasB = scannerB.Scan()
				continue
			}
			if dataB.Key <= dataC.Key {
				out.WriteString(lineB + "\n")
				hasB = scannerB.Scan()
			} else {
				out.WriteString(lineC + "\n")
				hasC = scannerC.Scan()
			}
		} else if hasB {
			if dataB.Key == keySize+1 {
				break
			}
			out.WriteString(lineB + "\n")
			hasB = scannerB.Scan()
		} else if hasC {
			if dataC.Key == keySize+1 {
				break
			}
			out.WriteString(lineC + "\n")
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
			return fmt.Errorf("Failed to distribute runs: %w", err)
		}

		if sorted {
			return nil
		}

		if err := mergeFiles(filePath, tempFileB, tempFileC); err != nil {
			return fmt.Errorf("failed to merge files: %w", err)
		}
	}
}

func main() {
	generateRandomFileA()
	sortFile("A.txt")
}
