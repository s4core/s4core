# Monitoring

S4 provides built-in observability through Prometheus-compatible metrics and a JSON stats API for dashboards and alerting.

- [Prometheus Metrics](prometheus-metrics.md) — what every node exports at `/metrics`, the cluster
  metrics and the alerts worth having, and a Grafana dashboard for a cluster
  ([`grafana/s4-cluster.json`](grafana/s4-cluster.json))
- [Stats API](stats-api.md) — a JSON summary for dashboard widgets and quick checks
