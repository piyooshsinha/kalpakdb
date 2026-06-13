// Package kalpak is a zero-dependency Go client for KalpakDB — a
// content-addressed, Raft-replicated database for AI agent state.
//
// Like the Python and TypeScript SDKs, key hashing happens server-side
// (POST /v1/keys), so the client needs no BLAKE3. Signed writes use the
// standard library's crypto/ed25519 over the canonical byte layout in
// crates/kalpak-core/src/signing.rs (locked by test; this file reproduces
// it byte-for-byte).
//
// Typical agent workflow:
//
//	db := kalpak.New("http://127.0.0.1:7411")
//	fp := kalpak.ModelFingerprint{ModelID: "meta-llama/Llama-3.1-8B", TokenizerHash: "tok", KVLayout: "fp16/paged-16"}
//	k0, _ := db.CacheKey(fp, []uint32{1, 2, 3})
//	k1, _ := db.ExtendKey(k0, []uint32{4, 5})
//	hit, _ := db.Lookup([]kalpak.CacheKey{k0, k1})
//	if hit == nil {
//	    ids, _ := db.PutBlocks([][]byte{chunk0, chunk1}) // one group commit
//	    db.BindChain(agent, []kalpak.ChainBinding{
//	        {Key: k0, Blocks: ids[:1]},
//	        {Key: k1, Blocks: ids, Parent: &k0},          // lookahead link
//	    })
//	}
package kalpak

import (
	"bytes"
	"crypto/ed25519"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// ModelFingerprint scopes a cache key to an exact model/tokenizer/layout;
// KV caches are not portable across any of the three.
type ModelFingerprint struct {
	ModelID       string `json:"model_id"`
	TokenizerHash string `json:"tokenizer_hash"`
	KVLayout      string `json:"kv_layout"`
}

// CacheKey is a (fingerprint, chained prefix hash) pair.
type CacheKey struct {
	Fingerprint ModelFingerprint `json:"fingerprint"`
	PrefixHash  string           `json:"prefix_hash"`
}

// PrefixHit is the longest cached prefix found for a key chain.
type PrefixHit struct {
	// Depth is the index into the queried chain of the deepest bound key.
	Depth int
	// Blocks are the hex content ids materializing that prefix.
	Blocks []string
}

// ChainBinding is one link in an atomic chain bind. Parent, when set,
// links it into the server-side prefix tree for lookahead prefetch.
type ChainBinding struct {
	Key    CacheKey
	Blocks []string
	Parent *CacheKey
}

// Error carries the HTTP status from a server-side failure.
type Error struct {
	Status  int
	Message string
}

func (e *Error) Error() string { return fmt.Sprintf("kalpak: %d: %s", e.Status, e.Message) }

// Signer signs metadata mutations for nodes running --require-signatures.
type Signer struct {
	priv ed25519.PrivateKey
	// Agent is the hex public key — the identity this signer writes as.
	Agent string
}

// NewSigner builds a signer from a 32-byte Ed25519 seed.
func NewSigner(seed []byte) (*Signer, error) {
	if len(seed) != ed25519.SeedSize {
		return nil, fmt.Errorf("kalpak: ed25519 seed must be %d bytes", ed25519.SeedSize)
	}
	priv := ed25519.NewKeyFromSeed(seed)
	pub := priv.Public().(ed25519.PublicKey)
	return &Signer{priv: priv, Agent: hex.EncodeToString(pub)}, nil
}

func (s *Signer) signHex(msg []byte) string {
	return hex.EncodeToString(ed25519.Sign(s.priv, msg))
}

// ---- Canonical signed-write messages -------------------------------------
// Byte-identical to kalpak-core::signing.

func u32le(n int) []byte {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, uint32(n))
	return b
}

func mustHex(s string) []byte {
	b, _ := hex.DecodeString(s)
	return b
}

func msgKey(buf *bytes.Buffer, k CacheKey) {
	buf.WriteString(k.Fingerprint.ModelID)
	buf.WriteByte(0)
	buf.WriteString(k.Fingerprint.TokenizerHash)
	buf.WriteByte(0)
	buf.WriteString(k.Fingerprint.KVLayout)
	buf.WriteByte(0)
	buf.Write(mustHex(k.PrefixHash))
}

func msgLink(buf *bytes.Buffer, key CacheKey, blocks []string, parent *CacheKey) {
	msgKey(buf, key)
	buf.Write(u32le(len(blocks)))
	for _, b := range blocks {
		buf.Write(mustHex(b))
	}
	if parent != nil {
		buf.WriteByte(1)
		buf.Write(mustHex(parent.PrefixHash))
	} else {
		buf.WriteByte(0)
	}
}

func registerMessage(agent, displayName string) []byte {
	var b bytes.Buffer
	b.WriteString("KLPK/reg/v1\x00")
	b.Write(mustHex(agent))
	b.WriteString(displayName)
	return b.Bytes()
}

func bindMessage(agent string, key CacheKey, blocks []string, parent *CacheKey) []byte {
	var b bytes.Buffer
	b.WriteString("KLPK/bind/v1\x00")
	b.Write(mustHex(agent))
	msgLink(&b, key, blocks, parent)
	return b.Bytes()
}

func chainMessage(agent string, links []ChainBinding) []byte {
	var b bytes.Buffer
	b.WriteString("KLPK/chain/v1\x00")
	b.Write(mustHex(agent))
	b.Write(u32le(len(links)))
	for _, l := range links {
		msgLink(&b, l.Key, l.Blocks, l.Parent)
	}
	return b.Bytes()
}

// ---- Client ----------------------------------------------------------------

// Client is a KalpakDB HTTP client. The zero value is not usable; call New.
type Client struct {
	base   string
	http   *http.Client
	signer *Signer
}

// Option configures a Client.
type Option func(*Client)

// WithSigner signs every metadata mutation (required by
// --require-signatures nodes).
func WithSigner(s *Signer) Option { return func(c *Client) { c.signer = s } }

// WithHTTPClient overrides the default *http.Client (e.g. for custom TLS).
func WithHTTPClient(h *http.Client) Option { return func(c *Client) { c.http = h } }

// New returns a client for the node at base.
func New(base string, opts ...Option) *Client {
	c := &Client{
		base: strings.TrimRight(base, "/"),
		http: &http.Client{Timeout: 30 * time.Second},
	}
	for _, o := range opts {
		o(c)
	}
	return c
}

// Signer returns the configured signer, or nil.
func (c *Client) Signer() *Signer { return c.signer }

func (c *Client) do(method, path, ctype string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequest(method, c.base+path, body)
	if err != nil {
		return nil, err
	}
	if ctype != "" {
		req.Header.Set("Content-Type", ctype)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 400 {
		defer resp.Body.Close()
		msg := fmt.Sprintf("%d", resp.StatusCode)
		var e struct {
			Error string `json:"error"`
		}
		if json.NewDecoder(resp.Body).Decode(&e) == nil && e.Error != "" {
			msg = e.Error
		}
		return nil, &Error{Status: resp.StatusCode, Message: msg}
	}
	return resp, nil
}

func (c *Client) postJSON(path string, payload any, out any) error {
	buf, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	resp, err := c.do("POST", path, "application/json", bytes.NewReader(buf))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}

func (c *Client) getJSON(path string, out any) error {
	resp, err := c.do("GET", path, "", nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return json.NewDecoder(resp.Body).Decode(out)
}

// -- keys (hashed server-side) --

// CacheKey computes the root prefix key for a token sequence.
func (c *Client) CacheKey(fp ModelFingerprint, tokens []uint32) (CacheKey, error) {
	var out CacheKey
	err := c.postJSON("/v1/keys", map[string]any{"fingerprint": fp, "tokens": tokens}, &out)
	return out, err
}

// ExtendKey chains a deeper prefix key onto parent.
func (c *Client) ExtendKey(parent CacheKey, tokens []uint32) (CacheKey, error) {
	var out CacheKey
	err := c.postJSON("/v1/keys", map[string]any{
		"fingerprint": parent.Fingerprint, "tokens": tokens, "parent": parent,
	}, &out)
	return out, err
}

// -- data plane --

// PutBlock stores one block and returns its content address. Idempotent.
func (c *Client) PutBlock(payload []byte) (string, error) {
	resp, err := c.do("POST", "/v1/blocks", "application/octet-stream", bytes.NewReader(payload))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var r struct {
		ID string `json:"id"`
	}
	err = json.NewDecoder(resp.Body).Decode(&r)
	return r.ID, err
}

// PutBlocks stores a batch under one group-committed fsync, returning
// content ids in input order.
func (c *Client) PutBlocks(payloads [][]byte) ([]string, error) {
	var b bytes.Buffer
	b.Write(u32le(len(payloads)))
	for _, p := range payloads {
		b.Write(u32le(len(p)))
		b.Write(p)
	}
	resp, err := c.do("POST", "/v1/blocks/batch", "application/octet-stream", &b)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var r struct {
		IDs []string `json:"ids"`
	}
	err = json.NewDecoder(resp.Body).Decode(&r)
	return r.IDs, err
}

// GetBlock fetches a block by content address.
func (c *Client) GetBlock(id string) ([]byte, error) {
	resp, err := c.do("GET", "/v1/blocks/"+id, "", nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

// -- control plane --

// RegisterAgent registers (or re-registers) an agent identity.
func (c *Client) RegisterAgent(agent, displayName string) error {
	body := map[string]any{"agent": agent, "display_name": displayName}
	if c.signer != nil {
		body["signature"] = c.signer.signHex(registerMessage(agent, displayName))
	}
	return c.postJSON("/v1/agents", body, nil)
}

// BindPrefix binds a prefix to its blocks; parent links the prefix tree
// for one-step-ahead speculative prefetch.
func (c *Client) BindPrefix(agent string, key CacheKey, blocks []string, parent *CacheKey) error {
	body := map[string]any{"agent": agent, "key": key, "blocks": blocks, "parent": parent}
	if c.signer != nil {
		body["signature"] = c.signer.signHex(bindMessage(agent, key, blocks, parent))
	}
	return c.postJSON("/v1/manifest/bind", body, nil)
}

// BindChain binds a whole root-first chain atomically (one consensus round).
func (c *Client) BindChain(agent string, bindings []ChainBinding) error {
	out := make([]map[string]any, len(bindings))
	for i, b := range bindings {
		out[i] = map[string]any{"key": b.Key, "blocks": b.Blocks, "parent": b.Parent}
	}
	body := map[string]any{"agent": agent, "bindings": out}
	if c.signer != nil {
		body["signature"] = c.signer.signHex(chainMessage(agent, bindings))
	}
	return c.postJSON("/v1/manifest/bind-chain", body, nil)
}

// Lookup probes a root-first key chain for the longest cached prefix; the
// server speculatively warms the hit's (and its children's) blocks. Returns
// nil when nothing is bound.
func (c *Client) Lookup(chain []CacheKey) (*PrefixHit, error) {
	var r struct {
		HitDepth *int     `json:"hit_depth"`
		Blocks   []string `json:"blocks"`
	}
	if err := c.postJSON("/v1/manifest/lookup", map[string]any{"chain": chain}, &r); err != nil {
		return nil, err
	}
	if r.HitDepth == nil {
		return nil, nil
	}
	return &PrefixHit{Depth: *r.HitDepth, Blocks: r.Blocks}, nil
}

// -- observability --

// Stats returns the node's data-plane and control-plane statistics.
func (c *Client) Stats() (map[string]any, error) {
	var out map[string]any
	err := c.getJSON("/v1/stats", &out)
	return out, err
}

// ListAgents returns the registered agents (the memory-explorer listing).
func (c *Client) ListAgents() ([]map[string]any, error) {
	var r struct {
		Agents []map[string]any `json:"agents"`
	}
	err := c.getJSON("/v1/agents/list", &r)
	return r.Agents, err
}

// AgentBindings returns one agent's prefix bindings.
func (c *Client) AgentBindings(agent string) ([]map[string]any, error) {
	var r struct {
		Bindings []map[string]any `json:"bindings"`
	}
	err := c.getJSON("/v1/agents/"+agent+"/bindings", &r)
	return r.Bindings, err
}
