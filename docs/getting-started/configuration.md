# Configuration

This guide covers how to configure RHAPSODY for different environments and use cases.

## Overview

RHAPSODY can be configured through multiple methods:

- Configuration files (YAML/JSON)
- Environment variables
- Programmatic configuration
- Command-line arguments

## Configuration Files

### Basic Configuration File

Create a `rhapsody.yaml` file in your project directory:

```yaml
# Basic RHAPSODY configuration
session:
  name: "my_workflow"
  working_directory: "/tmp/rhapsody_work"
  cleanup_on_exit: true

backend:
  type: "dask"
  max_workers: 4

logging:
  level: "INFO"
  format: "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
  file: "rhapsody.log"

monitoring:
  enable_callbacks: true
  progress_interval: 5.0
```

### Advanced Configuration

```yaml
# Advanced configuration with multiple backends
backends:
  dask:
    max_workers: 8
    scheduler_address: "tcp://localhost:8786"
    client_kwargs:
      timeout: 30

  radical_pilot:
    mongodb_url: "mongodb://localhost:27017/radical_pilot"
    resource_configs:
      - resource: "local.localhost"
        cores_per_node: 4
        nodes: 1

# Resource requirements
resources:
  default:
    cpu_cores: 1
    memory: "1GB"
    walltime: "00:10:00"

  compute_intensive:
    cpu_cores: 8
    memory: "16GB"
    walltime: "01:00:00"

  gpu_tasks:
    gpu_cores: 1
    cpu_cores: 4
    memory: "8GB"
```

## Environment Variables

Configure RHAPSODY using environment variables:

```bash
# Backend configuration
export RHAPSODY_BACKEND=dask
export RHAPSODY_MAX_WORKERS=8

# Logging configuration
export RHAPSODY_LOG_LEVEL=DEBUG
export RHAPSODY_LOG_FILE=/var/log/rhapsody.log

# Database configuration (for RADICAL-Pilot)
export RADICAL_PILOT_DBURL=mongodb://localhost:27017/radical_pilot

# Working directories
export RHAPSODY_WORK_DIR=/scratch/rhapsody
export RHAPSODY_TEMP_DIR=/tmp
```

## Programmatic Configuration

Configure RHAPSODY directly in your Python code:

```python
import rhapsody
from rhapsody.backends import Session
from rhapsody.config import RhapsodyConfig

# Create configuration object
config = RhapsodyConfig()
config.backend.type = "dask"
config.backend.max_workers = 4
config.logging.level = "INFO"
config.session.working_directory = "/tmp/my_workflow"

# Create session with configuration
session = Session(config=config)
backend = rhapsody.get_backend("dask", config=config)
```

## Backend-Specific Configuration

### Dask Backend Configuration

```yaml
backends:
  dask:
    max_workers: 8
    scheduler_address: null  # null for local cluster
    dashboard_address: ":8787"
    client_kwargs:
      timeout: "30s"
      heartbeat_interval: "5s"
    cluster_kwargs:
      n_workers: 4
      threads_per_worker: 2
      memory_limit: "4GB"
```

### RADICAL-Pilot Backend Configuration

```yaml
backends:
  radical_pilot:
    mongodb_url: "mongodb://localhost:27017/radical_pilot"
    resource_configs:
      - resource: "local.localhost"
        runtime: 30
        cores: 4
        gpus: 0
      - resource: "slurm.cluster.edu"
        runtime: 120
        cores: 128
        queue: "normal"
        project: "allocation_id"
```

### Backend Selection in Code

```python
# Choose backend based on environment
import os

backend_type = os.environ.get("RHAPSODY_BACKEND", "dask")

if backend_type == "dask":
    backend = rhapsody.get_backend("dask")
elif backend_type == "radical_pilot":
    hpc_resources = {
        "resource": "local.localhost",
        "runtime": 30,
        "cores": 4
    }
    backend = await rhapsody.get_backend("radical_pilot", hpc_resources)
```

### Dask Backend

```python
# Dask backend configuration
dask_config = {
    "scheduler_address": "tcp://scheduler:8786",
    "client_kwargs": {
        "timeout": 60,
        "heartbeat_interval": 5000,
        "security": None
    },
    "cluster_kwargs": {
        "n_workers": 4,
        "threads_per_worker": 2,
        "memory_limit": "4GB"
    }
}

backend = rhapsody.get_backend("dask", config=dask_config)
```

### RADICAL-Pilot Backend

```python
# RADICAL-Pilot backend configuration
rp_config = {
    "mongodb_url": "mongodb://localhost:27017/radical_pilot",
    "resource_configs": [
        {
            "resource": "ornl.summit",
            "queue": "batch",
            "cores_per_node": 42,
            "nodes": 10,
            "walltime": 120,  # minutes
            "project": "MY_PROJECT"
        }
    ],
    "pilot_description": {
        "runtime": 60,
        "cores": 168,
        "cleanup": True
    }
}

backend = rhapsody.get_backend("radical_pilot", config=rp_config)
```

## Platform-Specific Configuration

### HPC Cluster Configuration

```yaml
# Configuration for SLURM-based HPC systems
platform: "slurm"

slurm:
  partition: "gpu"
  account: "my_project"
  qos: "normal"
  constraint: "haswell"

resources:
  default:
    nodes: 1
    cpu_cores: 24
    memory: "64GB"
    walltime: "02:00:00"

modules:
  - "python/3.9"
  - "cuda/11.2"
  - "openmpi/4.1.0"

environment:
  CUDA_VISIBLE_DEVICES: "0,1"
  OMP_NUM_THREADS: "8"
```

### Cloud Configuration

```yaml
# Configuration for cloud environments
platform: "cloud"

cloud:
  provider: "aws"
  region: "us-west-2"
  instance_type: "c5.xlarge"

resources:
  default:
    cpu_cores: 4
    memory: "8GB"

storage:
  input_bucket: "s3://my-input-data"
  output_bucket: "s3://my-results"
  temp_storage: "/tmp"
```

## Loading Configuration

### From Files

```python
import rhapsody
from rhapsody.config import load_config

# Load from YAML file
config = load_config("rhapsody.yaml")

# Load from JSON file
config = load_config("rhapsody.json")

# Load from multiple files (later files override earlier ones)
config = load_config(["base.yaml", "environment.yaml", "local.yaml"])
```

### From Environment

```python
import rhapsody
from rhapsody.config import load_config_from_env

# Load configuration from environment variables
config = load_config_from_env()

# Load with prefix
config = load_config_from_env(prefix="RHAPSODY_")
```

## Configuration Validation

RHAPSODY validates configuration at startup:

```python
from rhapsody.config import validate_config

try:
    config = load_config("rhapsody.yaml")
    validate_config(config)
    print("Configuration is valid")
except ConfigurationError as e:
    print(f"Configuration error: {e}")
```

## Configuration Templates

### Development Template

```yaml
# Development configuration
session:
  name: "dev_workflow"
  working_directory: "./work"
  cleanup_on_exit: true

backend:
  type: "dask"
  max_workers: 2

logging:
  level: "DEBUG"
  console: true
  file: "dev.log"
```

### Production Template

```yaml
# Production configuration
session:
  name: "prod_workflow"
  working_directory: "/data/workflows"
  cleanup_on_exit: false

backend:
  type: "radical_pilot"

logging:
  level: "INFO"
  console: false
  file: "/var/log/rhapsody/prod.log"

monitoring:
  enable_metrics: true
  metrics_port: 9090
```

## Best Practices

1. **Use configuration files** for complex setups
2. **Environment variables** for deployment-specific values
3. **Version control** your configuration files
4. **Validate configurations** before deployment
5. **Use templates** for different environments
6. **Document configuration** options and their effects

## Troubleshooting Configuration

### Common Issues

**Configuration file not found:**
```python
# Specify full path
config = load_config("/absolute/path/to/rhapsody.yaml")

# Check current directory
import os
print(os.getcwd())
```

**Invalid configuration values:**
```python
# Enable validation
from rhapsody.config import RhapsodyConfig

config = RhapsodyConfig()
config.validate()  # Raises exception for invalid values
```

**Backend not available:**
```python
# Check available backends
print(rhapsody.list_backends())

# Install missing dependencies
# pip install "rhapsody[dask]" for Dask backend
```

For more detailed information about RHAPSODY's configuration options, refer to the [API Reference](../reference/configuration.md).
