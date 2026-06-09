"""
Prometheus Pushgateway helper for pipeline stage metrics.
Errors are silently swallowed — observability must never break the pipeline.
"""
import os

PUSHGATEWAY_URL = os.environ.get("PUSHGATEWAY_URL", "http://pushgateway:9091")


def push_metrics(job: str, **metrics: float) -> None:
    """Push name=value pairs to the Pushgateway under the given job label."""
    try:
        from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

        registry = CollectorRegistry()
        for name, value in metrics.items():
            Gauge(name, name.replace("_", " "), registry=registry).set(value)
        push_to_gateway(PUSHGATEWAY_URL, job=job, registry=registry)
    except Exception:
        pass
