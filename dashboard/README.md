# Kalpak Dashboard

React-based observability dashboard (Phase 4 of the roadmap — see
[docs/ARCHITECTURE.md](../docs/ARCHITECTURE.md)).

Planned responsibilities:

- Real-time WebSocket streams of Raft log commits and data-plane throughput
- Agent data lineage and memory-state inspection
- Cluster health and tier-occupancy views

Scaffolding (Vite + React + TypeScript) will land once `kalpakd` exposes its
first metrics endpoint; a dashboard with nothing to observe is just a mockup.
