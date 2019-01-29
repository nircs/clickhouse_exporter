package exporter

import (
	"fmt"
	"strconv"
	"strings"
)

type partsPartitionResult struct {
	database  string
	table     string
	partition string
	bytes     int
	parts     int
	rows      int
}

func (e *Exporter) parsePartsPartitionResponse(uri string) ([]partsPartitionResult, error) {
	data, err := e.handleResponse(uri)
	if err != nil {
		return nil, err
	}

	// Parsing results
	lines := strings.Split(string(data), "\n")
	var results = make([]partsPartitionResult, 0)

	for i, line := range lines {
		parts := strings.Fields(line)
		if len(parts) == 0 {
			continue
		}
		if len(parts) != 6 {
			return nil, fmt.Errorf("parsePartsResponse: unexpected %d line: %s", i, line)
		}
		database := strings.TrimSpace(parts[0])
		table := strings.TrimSpace(parts[1])
		partition := strings.TrimSpace(parts[2])

		bytes, err := strconv.Atoi(strings.TrimSpace(parts[3]))
		if err != nil {
			return nil, err
		}

		count, err := strconv.Atoi(strings.TrimSpace(parts[4]))
		if err != nil {
			return nil, err
		}

		rows, err := strconv.Atoi(strings.TrimSpace(parts[5]))
		if err != nil {
			return nil, err
		}

		results = append(results, partsPartitionResult{database, table, partition, bytes, count, rows})
	}
	return results, nil
}
