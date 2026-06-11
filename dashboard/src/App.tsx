import { useEffect, useRef, useState } from "react";

type Stats = {
  data_plane: {
    blocks: number;
    segments: number;
    bytes_on_disk: number;
    warm_blocks: number;
    warm_bytes: number;
    warm_budget: number;
    hits: number;
    misses: number;
  };
  control_plane: {
    node_id: number;
    leader: number | null;
    term: number;
    last_log_index: number | null;
    last_applied: number | null;
    agents: number;
    bindings: number;
    peers: string[];
  };
};

function fmtBytes(n: number): string {
  if (n >= 1 << 30) return (n / (1 << 30)).toFixed(1) + " GiB";
  if (n >= 1 << 20) return (n / (1 << 20)).toFixed(1) + " MiB";
  if (n >= 1 << 10) return (n / (1 << 10)).toFixed(1) + " KiB";
  return n + " B";
}

function Card(props: { label: string; value: string; detail?: string; children?: React.ReactNode }) {
  return (
    <div className="card">
      <div className="label">{props.label}</div>
      <div className="value">{props.value}</div>
      {props.detail && <div className="detail">{props.detail}</div>}
      {props.children}
    </div>
  );
}

function Sparkline({ points }: { points: number[] }) {
  if (points.length < 2) return <svg className="spark" />;
  const max = Math.max(...points, 1);
  const path = points
    .map((p, i) => `${(i / (points.length - 1)) * 100},${36 - (p / max) * 32}`)
    .join(" ");
  return (
    <svg className="spark" viewBox="0 0 100 36" preserveAspectRatio="none">
      <polyline points={path} />
    </svg>
  );
}

export default function App() {
  const [stats, setStats] = useState<Stats | null>(null);
  const [online, setOnline] = useState(false);
  const history = useRef<number[]>([]);

  useEffect(() => {
    let ws: WebSocket | null = null;
    let retry: ReturnType<typeof setTimeout>;

    const connect = () => {
      const proto = location.protocol === "https:" ? "wss" : "ws";
      ws = new WebSocket(`${proto}://${location.host}/v1/ws`);
      ws.onopen = () => setOnline(true);
      ws.onmessage = (ev) => {
        const s: Stats = JSON.parse(ev.data);
        const total = s.data_plane.hits + s.data_plane.misses;
        history.current = [...history.current.slice(-59), total];
        setStats(s);
      };
      ws.onclose = () => {
        setOnline(false);
        retry = setTimeout(connect, 2000);
      };
    };
    connect();
    return () => {
      clearTimeout(retry);
      ws?.close();
    };
  }, []);

  const d = stats?.data_plane;
  const c = stats?.control_plane;
  const hitTotal = d ? d.hits + d.misses : 0;
  const hitRate = d && hitTotal > 0 ? ((d.hits / hitTotal) * 100).toFixed(1) + "%" : "—";
  const warmPct = d && d.warm_budget > 0 ? (d.warm_bytes / d.warm_budget) * 100 : 0;

  return (
    <div className="app">
      <header>
        <h1>Kalpak</h1>
        <span className="sub">distributed agentic context fabric</span>
        <span className={`status ${online ? "online" : "offline"}`}>
          {online ? "● live" : "○ connecting…"}
        </span>
      </header>

      <section>
        <h2>Data plane</h2>
        <div className="grid">
          <Card
            label="Blocks"
            value={d ? String(d.blocks) : "—"}
            detail={d ? `${d.segments} segment(s), ${fmtBytes(d.bytes_on_disk)} on disk` : ""}
          />
          <Card
            label="Warm tier"
            value={d ? fmtBytes(d.warm_bytes) : "—"}
            detail={d ? `${d.warm_blocks} block(s) of ${fmtBytes(d.warm_budget)} budget` : ""}
          >
            <div className="bar">
              <div style={{ width: `${Math.min(warmPct, 100)}%` }} />
            </div>
          </Card>
          <Card label="Warm hit rate" value={hitRate} detail={d ? `${d.hits} hits / ${d.misses} misses` : ""}>
            <Sparkline points={history.current} />
          </Card>
        </div>
      </section>

      <section>
        <h2>Control plane (Raft)</h2>
        <div className="grid">
          <Card
            label="Leader"
            value={c ? (c.leader != null ? `node ${c.leader}` : "none") : "—"}
            detail={c ? `this node: ${c.node_id}, term ${c.term}` : ""}
          />
          <Card
            label="Log"
            value={c && c.last_log_index != null ? `#${c.last_log_index}` : "—"}
            detail={c && c.last_applied != null ? `applied through #${c.last_applied}` : ""}
          />
          <Card label="Agents" value={c ? String(c.agents) : "—"} detail="registered identities" />
          <Card label="Prefix bindings" value={c ? String(c.bindings) : "—"} detail="CacheKey → blocks" />
          <Card
            label="Cluster"
            value={c ? `${(c.peers?.length ?? 0) + 1} node(s)` : "—"}
            detail={c && c.peers?.length ? `peers: ${c.peers.join(", ")}` : "single node"}
          />
        </div>
      </section>
    </div>
  );
}
