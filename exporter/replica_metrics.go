package exporter

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
)

type replicaMetrics struct {
	metrics []*replicaMetric
}

func NewReplicaMetrics() *replicaMetrics {
	return &replicaMetrics{
		metrics: []*replicaMetric{
			newReplicaMetric("is_readonly", "Whether the replica is in read-only mode."),
			newReplicaMetric("is_session_expired", "Whether the ZK session expired."),
			newReplicaMetric("future_parts", "The number of data parts that will appear as the result of INSERTs or merges that haven't been done yet."),
			newReplicaMetric("parts_to_check", "The number of data parts in the queue for verification."),
			newReplicaMetric("queue_size", "Size of the queue for operations waiting to be performed."),
			newReplicaMetric("inserts_in_queue", "Number of inserts of blocks of data that need to be made."),
			newReplicaMetric("log_max_index", "Maximum entry number in the log of general activity."),
			newReplicaMetric("log_pointer", "Maximum entry number in the log of general activity that the replica copied to its execution queue, plus one."),
			newReplicaMetric("active_replicas", "number of active replicas"),
			newReplicaMetric("total_replicas", "total number of replicas"),
		},
	}
}

func (r *replicaMetrics) size() int {
	return len(r.metrics)
}

func (r *replicaMetrics) populate(parts []string) error {
	for i := 0; i < r.size(); i++ {
		r.metrics[i].database = strings.TrimSpace(parts[0])
		r.metrics[i].table = strings.TrimSpace(parts[1])

		value, err := strconv.ParseFloat(strings.TrimSpace(parts[i+2]), 64)
		if err != nil {
			return err
		}
		r.metrics[i].value = value
	}
	return nil
}

func (r *replicaMetrics) listMetrics() string {
	var result = make([]string, 0, r.size())
	for _, metric := range r.metrics {
		result = append(result, metric.name)
	}
	return strings.Join(result, ", ")
}

func (r *replicaMetrics) exposeMetrics(ch chan<- prometheus.Metric) {
	for _, metric := range r.metrics {
		metric.exposeMetric(ch)
	}
}

type replicaMetric struct {
	name     string
	help     string
	database string
	table    string
	value    float64
}

func newReplicaMetric(name string, help string) *replicaMetric {
	return &replicaMetric{
		name: name,
		help: name + " : " + help,
	}
}

func (r *replicaMetric) exposeMetric(ch chan<- prometheus.Metric) {
	newMetric := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      r.name,
		Help:      fmt.Sprintf("%s : %s", r.name, r.help),
	}, []string{"database", "table"}).WithLabelValues(r.database, r.table)
	newMetric.Set(r.value)
	newMetric.Collect(ch)
}

func (e *Exporter) parseReplicasResponse(uri string) ([]*replicaMetrics, error) {
	data, err := e.handleResponse(uri)
	if err != nil {
		return nil, err
	}

	// Parsing results
	lines := strings.Split(string(data), "\n")
	var results = make([]*replicaMetrics, 0, len(lines))

	for i, line := range lines {

		var replicaMetrics = NewReplicaMetrics()
		parts := strings.Fields(line)
		if len(parts) == 0 {
			continue
		}
		// Compare number of columns with number of metrics + 2(database and table fields)
		if len(parts) != (replicaMetrics.size() + 2) {
			return nil, fmt.Errorf("parseReplicasResponse: unexpected %d line: %s", i, line)
		}

		if err := replicaMetrics.populate(parts); err != nil {
			return nil, err
		}

		results = append(results, replicaMetrics)
	}

	return results, nil
}
