# Configuration API Reference

API documentation for RHAPSODY configuration management.

## Backend Configuration

Configuration for RHAPSODY backends is handled through constructor parameters and resource specifications.

### Dask Backend Configuration

Dask backend accepts configuration through keyword arguments:

```python
backend = rhapsody.get_backend("dask",
                              scheduler_address="tcp://localhost:8786",
                              dashboard_address=":8787")
```

### RADICAL-Pilot Backend Configuration

RADICAL-Pilot backend requires resource configuration:

```python
hpc_resources = {
    "resource": "local.localhost",
    "runtime": 30,
    "cores": 4
}
backend = await rhapsody.get_backend("radical_pilot", hpc_resources)
```

## Environment Variables

See the [Configuration Guide](../getting-started/configuration.md) for environment variable options.
