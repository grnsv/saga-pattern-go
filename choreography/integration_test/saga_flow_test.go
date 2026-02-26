//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go/modules/kafka"
)

var (
	sharedBroker string
	binDir       string
)

func TestMain(m *testing.M) {
	os.Exit(runTests(m))
}

func runTests(m *testing.M) int {
	ctx := context.Background()

	kc, err := kafka.Run(ctx, "confluentinc/cp-kafka:8.0.4",
		kafka.WithClusterID("test-cluster-id"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "start kafka: %v\n", err)
		return 1
	}
	defer func() {
		if termErr := kc.Terminate(ctx); termErr != nil {
			fmt.Fprintf(os.Stderr, "terminate kafka: %v\n", termErr)
		}
	}()

	brokers, err := kc.Brokers(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "get kafka brokers: %v\n", err)
		return 1
	}
	sharedBroker = brokers[0]

	// Pre-create all topics so that the first publish never hits UnknownTopicOrPartition.
	// Without this, the first write fails, the handler exhausts retries, and the uncommitted
	// offset causes cross-test event replay in subsequent tests.
	for _, topic := range []string{
		"order-events", "payment-events", "inventory-events",
		"order-events.dlq", "payment-events.dlq", "inventory-events.dlq",
	} {
		if exitCode, _, execErr := kc.Exec(ctx, []string{
			"kafka-topics",
			"--bootstrap-server", "localhost:9092",
			"--create", "--if-not-exists",
			"--topic", topic,
			"--partitions", "1",
			"--replication-factor", "1",
		}); execErr != nil || exitCode != 0 {
			fmt.Fprintf(os.Stderr, "create topic %s: exit=%d %v\n", topic, exitCode, execErr)
			return 1
		}
	}

	binDir, err = os.MkdirTemp("", "saga-integration-*")
	if err != nil {
		fmt.Fprintf(os.Stderr, "create temp dir: %v\n", err)
		return 1
	}
	defer func() {
		if rmErr := os.RemoveAll(binDir); rmErr != nil {
			fmt.Fprintf(os.Stderr, "cleanup temp dir: %v\n", rmErr)
		}
	}()

	for _, svc := range []struct{ dir, name string }{
		{"../order-service", "order-service"},
		{"../payment-service", "payment-service"},
		{"../inventory-service", "inventory-service"},
	} {
		out := filepath.Join(binDir, svc.name)
		cmd := exec.CommandContext(ctx, "go", "build", "-o", out, "./cmd/...")
		cmd.Dir = svc.dir
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			fmt.Fprintf(os.Stderr, "build %s: %v\n", svc.name, err)
			return 1
		}
	}

	return m.Run()
}

func startServices(t *testing.T, paymentChaos, inventoryChaos bool) (orderURL string, cleanup func()) {
	t.Helper()

	sysPath := os.Getenv("PATH")
	home := os.Getenv("HOME")

	// context.Background() is intentional: processes are managed via Kill() in cleanup.
	ctx := context.Background()

	newCmd := func(name, port string, extra ...string) *exec.Cmd {
		binPath := filepath.Join(binDir, name)
		cmd := exec.CommandContext(ctx, binPath) //nolint:gosec // path is under the controlled temp dir built by runTests
		cmd.Env = append([]string{
			"PATH=" + sysPath,
			"HOME=" + home,
			"HTTP_PORT=" + port,
			"KAFKA_BROKERS=" + sharedBroker,
			"OTEL_ENDPOINT=",
		}, extra...)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		return cmd
	}

	cmds := []*exec.Cmd{
		newCmd("order-service", "18080"),
		newCmd("payment-service", "18081",
			"SUCCESS_RATE=1.0",
			"CHAOS_MODE="+strconv.FormatBool(paymentChaos),
		),
		newCmd("inventory-service", "18082",
			"SUCCESS_RATE=1.0",
			"CHAOS_MODE="+strconv.FormatBool(inventoryChaos),
		),
	}

	for i, cmd := range cmds {
		if err := cmd.Start(); err != nil {
			for _, s := range cmds[:i] {
				_ = s.Process.Kill()
				_ = s.Wait()
			}
			t.Fatalf("start service: %v", err)
		}
	}

	waitForReady(t, "http://localhost:18080/readyz", 15*time.Second)
	waitForReady(t, "http://localhost:18081/readyz", 15*time.Second)
	waitForReady(t, "http://localhost:18082/readyz", 15*time.Second)

	return "http://localhost:18080", func() {
		for _, cmd := range cmds {
			if cmd.Process != nil {
				_ = cmd.Process.Kill()
				_ = cmd.Wait()
			}
		}
	}
}

func waitForReady(t *testing.T, url string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
		if err == nil {
			resp, doErr := http.DefaultClient.Do(req)
			if doErr == nil {
				resp.Body.Close()
				if resp.StatusCode == http.StatusOK {
					return
				}
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("service at %s not ready after %s", url, timeout)
}

func createOrder(t *testing.T, baseURL string) string {
	t.Helper()
	body := bytes.NewBufferString(`{"item":"widget","qty":1}`)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, baseURL+"/orders", body)
	if err != nil {
		t.Fatalf("create order request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("create order: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create order: unexpected status %d", resp.StatusCode)
	}

	var result struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode order response: %v", err)
	}
	return result.ID
}

func pollOrderStatus(baseURL, orderID string) string {
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet,
		fmt.Sprintf("%s/orders/%s", baseURL, orderID), nil)
	if err != nil {
		return ""
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return ""
	}
	defer resp.Body.Close()
	var result struct {
		Status string `json:"status"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return ""
	}
	return result.Status
}

func waitForStatus(t *testing.T, baseURL, orderID, wantStatus string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastStatus string
	for time.Now().Before(deadline) {
		if status := pollOrderStatus(baseURL, orderID); status != "" {
			lastStatus = status
			if status == wantStatus {
				return
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("order %s: want status %q, got %q after %s", orderID, wantStatus, lastStatus, timeout)
}

func TestSaga(t *testing.T) {
	tests := []struct {
		name           string
		paymentChaos   bool
		inventoryChaos bool
		wantStatus     string
	}{
		{"HappyPath", false, false, "CONFIRMED"},
		{"PaymentFailure", true, false, "CANCELLED"},
		{"InventoryFailure", false, true, "CANCELLED"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			orderURL, cleanup := startServices(t, tc.paymentChaos, tc.inventoryChaos)
			defer cleanup()

			orderID := createOrder(t, orderURL)
			waitForStatus(t, orderURL, orderID, tc.wantStatus, 60*time.Second)
		})
	}
}
