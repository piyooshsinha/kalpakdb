// End-to-end tests against a real kalpakdb node (KALPAKDB_BIN), covering
// the full agent workflow and cross-language signed writes (Go's stdlib
// crypto/ed25519 vs the Rust verifier).
package kalpak

import (
	"bytes"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

const (
	plainAddr  = "127.0.0.1:17641"
	signedAddr = "127.0.0.1:17642"
)

func bin() string {
	if b := os.Getenv("KALPAKDB_BIN"); b != "" {
		return b
	}
	return "../../target/debug/kalpakdb"
}

// startNode spawns a node and returns a cleanup func; it waits until the
// node answers /v1/stats.
func startNode(t *testing.T, addr string, extra ...string) func() {
	t.Helper()
	dir := t.TempDir()
	args := append([]string{"serve", dir, "--addr", addr, "--compact-secs", "0"}, extra...)
	cmd := exec.Command(bin(), args...)
	if err := cmd.Start(); err != nil {
		t.Fatalf("spawn node: %v", err)
	}
	db := New("http://" + addr)
	deadline := time.Now().Add(15 * time.Second)
	for {
		if _, err := db.Stats(); err == nil {
			break
		}
		if time.Now().After(deadline) {
			_ = cmd.Process.Kill()
			t.Fatalf("node at %s did not start", addr)
		}
		time.Sleep(150 * time.Millisecond)
	}
	return func() { _ = cmd.Process.Kill(); _, _ = cmd.Process.Wait() }
}

func TestAgentWorkflow(t *testing.T) {
	defer startNode(t, plainAddr)()
	db := New("http://" + plainAddr)

	agent := strings.Repeat("0e", 32)
	if err := db.RegisterAgent(agent, "go-agent"); err != nil {
		t.Fatal(err)
	}

	fp := ModelFingerprint{ModelID: "test/model", TokenizerHash: "tok", KVLayout: "fp16/paged-16"}
	k0, err := db.CacheKey(fp, []uint32{1, 2, 3})
	if err != nil {
		t.Fatal(err)
	}
	k1, err := db.ExtendKey(k0, []uint32{4, 5})
	if err != nil {
		t.Fatal(err)
	}

	if hit, _ := db.Lookup([]CacheKey{k0, k1}); hit != nil {
		t.Fatal("fresh prefix must miss")
	}

	chunk0 := []byte("kv-go-chunk-0")
	chunk1 := []byte("kv-go-chunk-1")
	ids, err := db.PutBlocks([][]byte{chunk0, chunk1}) // one group commit
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 {
		t.Fatalf("want 2 ids, got %d", len(ids))
	}

	if err := db.BindChain(agent, []ChainBinding{
		{Key: k0, Blocks: ids[:1]},
		{Key: k1, Blocks: ids, Parent: &k0},
	}); err != nil {
		t.Fatal(err)
	}

	hit, err := db.Lookup([]CacheKey{k0, k1})
	if err != nil {
		t.Fatal(err)
	}
	if hit == nil || hit.Depth != 1 {
		t.Fatalf("want depth 1 hit, got %+v", hit)
	}

	got0, _ := db.GetBlock(ids[0])
	got1, _ := db.GetBlock(ids[1])
	if !bytes.Equal(got0, chunk0) || !bytes.Equal(got1, chunk1) {
		t.Fatal("fetched blocks differ from what was stored")
	}

	agents, err := db.ListAgents()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, a := range agents {
		if a["agent"] == agent {
			found = true
		}
	}
	if !found {
		t.Fatal("agent missing from registry")
	}
	bindings, _ := db.AgentBindings(agent)
	if len(bindings) != 2 {
		t.Fatalf("want 2 bindings, got %d", len(bindings))
	}
}

func TestSignedWrites(t *testing.T) {
	defer startNode(t, signedAddr, "--require-signatures")()

	seed := bytes.Repeat([]byte{7}, 32)
	signer, err := NewSigner(seed)
	if err != nil {
		t.Fatal(err)
	}

	// Unsigned client is rejected with 401.
	unsigned := New("http://" + signedAddr)
	err = unsigned.RegisterAgent(signer.Agent, "nope")
	if e, ok := err.(*Error); !ok || e.Status != 401 {
		t.Fatalf("unsigned register: want 401, got %v", err)
	}

	// Go-signed mutations must verify against the Rust node: the canonical
	// message bytes and Ed25519 signature have to match exactly.
	db := New("http://"+signedAddr, WithSigner(signer))
	if err := db.RegisterAgent(signer.Agent, "go-signed"); err != nil {
		t.Fatal(err)
	}
	id, err := db.PutBlock([]byte("kv-signed-go"))
	if err != nil {
		t.Fatal(err)
	}
	fp := ModelFingerprint{ModelID: "test/model", TokenizerHash: "tok", KVLayout: "fp16/paged-16"}
	k0, _ := db.CacheKey(fp, []uint32{9, 9})
	k1, _ := db.ExtendKey(k0, []uint32{8})
	if err := db.BindPrefix(signer.Agent, k0, []string{id}, nil); err != nil {
		t.Fatal(err)
	}
	if err := db.BindChain(signer.Agent, []ChainBinding{{Key: k1, Blocks: []string{id}, Parent: &k0}}); err != nil {
		t.Fatal(err)
	}
	hit, _ := db.Lookup([]CacheKey{k0, k1})
	if hit == nil || hit.Depth != 1 {
		t.Fatalf("signed chain bind not served: %+v", hit)
	}

	// A different key cannot write as this agent.
	intruderSeed := bytes.Repeat([]byte{8}, 32)
	intruderSigner, _ := NewSigner(intruderSeed)
	intruder := New("http://"+signedAddr, WithSigner(intruderSigner))
	err = intruder.RegisterAgent(signer.Agent, "forged")
	if e, ok := err.(*Error); !ok || e.Status != 401 {
		t.Fatalf("forged register: want 401, got %v", err)
	}
}
