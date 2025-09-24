# OBSERVE

[![CI](https://github.com/stride-research/observe/workflows/CI/badge.svg)](https://github.com/stride-research/observe/actions/workflows/ci.yml)
[![Examples](https://github.com/stride-research/observe/workflows/Examples/badge.svg)](https://github.com/stride-research/observe/actions/workflows/examples-nightly.yml)
[![Deploy Documentation](https://github.com/stride-research/observe/workflows/Deploy%20Documentation/badge.svg)](https://github.com/stride-research/observe/actions/workflows/docs.yml)
[![PyPI version](https://badge.fury.io/py/observe.svg)](https://badge.fury.io/py/observe)
[![Python versions](https://img.shields.io/pypi/pyversions/observe.svg)](https://pypi.org/project/observe/)
[![Downloads](https://pepy.tech/badge/observe)](https://pepy.tech/project/observe)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![Security: Bandit](https://img.shields.io/badge/security-bandit-yellow.svg)](https://github.com/PyCQA/bandit)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit)](https://github.com/pre-commit/pre-commit)

**O**bservability for **B**uilding **S**cientific **E**xperiments with **R**untime **V**isibility across **E**cosystems

A comprehensive observability Python package providing structured logging, distributed tracing, metrics collection, and high-performance serialization designed for scientific computing, HPC environments, and production systems.

## Features

- **Unified Async API**: Single async interface for all observability components (logging, tracing, metrics)
- **Environment-Aware Configuration**: Automatic optimization for HPC, development, production, and testing environments
- **High-Performance Serialization**: Multiple formats (JSON, Parquet, MessagePack, CSV) with intelligent batching and async writes
- **Structured Logging**: JSON logging with correlation IDs and automatic context propagation
- **Distributed Tracing**: Complete trace collection with span management and export capabilities
- **Comprehensive Metrics**: Counters, gauges, histograms with automatic collection and export
- **Flexible Output Strategies**: Support for workflow-based, component-based, and time-based data organization
- **Async-First Design**: Built for high-performance concurrent operations in scientific and production environments
- **CLI Tools**: Built-in CI testing and development utilities

## Quick Start

```python
import asyncio
from observe import initialize_observability, record_log, record_trace, record_metric, flush_all_data

async def main():
    # Initialize observability (auto-detects environment and optimizes settings)
    config = await initialize_observability("my_application")

    # Record structured logs with automatic context
    await record_log("Processing started", level="INFO",
                     extra={"batch_size": 1000, "algorithm": "fft"})

    # Record distributed traces
    await record_trace("data_analysis", duration=2.5,
                       attributes={"data_points": 10000, "result_size": 150})

    # Record metrics
    await record_metric("analyses_completed", value=1, metric_type="counter")
    await record_metric("analysis_duration_seconds", value=2.5, metric_type="histogram")

    await record_log("Analysis completed", level="INFO",
                     extra={"result_count": 150})

    # Ensure all data is written
    await flush_all_data()

# Run the async application
asyncio.run(main())
```

### Component-Based Usage

```python
import asyncio
from observe import initialize_observability, create_logger, create_tracer, create_metrics

async def main():
    # Initialize observability
    config = await initialize_observability("protein_folding_study")

    # Create components for different purposes
    logger = create_logger("data_processor")
    tracer = create_tracer("analysis_service")
    metrics = create_metrics("scientific_app")

    # Use components with async operations
    await logger.info("Starting molecular dynamics simulation")

    # Async metric operations
    counter = await metrics.counter("simulations_completed")
    await counter.inc()

    # Ensure all data is flushed
    from observe import flush_all_data
    await flush_all_data()

asyncio.run(main())
```

## Installation

```bash
# Basic installation
pip install observe

# With development tools
pip install observe[dev]

# With documentation tools
pip install observe[docs]

# With high-performance serialization
pip install observe[serialization]

# Everything (recommended for development)
pip install observe[all]
```

## Core Concepts

### Environment Detection

Observe automatically detects your environment and optimizes settings:

- **Development**: Human-readable JSON output, immediate flushing, verbose logging
- **HPC**: High-performance Parquet format, large batches, optimized for shared filesystems
- **Production**: Minimal overhead, efficient formats, reduced sampling rates
- **Testing**: Fast startup, minimal output, synchronous writes for predictable tests

### Output Strategies

Choose how observability data is organized:

- **workflow_based**: Organize by workflow/session (default for scientific computing)
- **component_based**: Separate directories for logs, traces, metrics
- **time_based**: Organize by timestamp for time-series analysis
- **flat**: Single directory for simple cases

### High-Performance Serialization

```python
import asyncio
from observe import initialize_observability, record_log

async def main():
    # Configure for your use case
    config = await initialize_observability(
        "my_application",
        output_format="parquet",       # parquet, json, msgpack, csv
        compression_level="balanced",   # none, fast, balanced
        batch_size=5000,               # Optimize for throughput
        flush_interval=30.0            # Async flush interval
    )

    # Record data with automatic batching
    await record_log("computation_finished",
                     level="INFO",
                     extra={"duration": 15.2, "memory_usage": "2.1GB"})

    # Ensure data is written
    from observe import flush_all_data
    await flush_all_data()

asyncio.run(main())
```

## CLI Tools

Observe includes command-line tools for development and CI testing:

### Local CI Testing

Test your changes locally before pushing to GitHub Actions:

```bash
# Quick validation (available now)
observe-test-ci

# Test specific environments
observe-test-ci --environments lint format type

# Run with fail-fast mode
observe-test-ci --fail-fast
```

**Features:**

- **Comprehensive testing** - All tox environments, Python matrix, integration tests
- **Failure detection** - Catch CI issues before GitHub Actions
- **Act integration** - Local GitHub Actions workflow testing
- **Flexible configuration** - Skip specific tests, fail-fast mode, custom environments

### Benchmarks (Development)

Performance benchmarks for API, components, and serialization formats:

```bash
# Format performance benchmarks
python -m observe.cli.benchmarks.formats

# API performance benchmarks
python -m observe.cli.benchmarks.api

# Component performance benchmarks
python -m observe.cli.benchmarks.components
```

**Available Benchmarks:**

- **Format Benchmarks**: Compare serialization performance, compression ratios, and memory usage across JSON, Parquet, MessagePack, and CSV formats
- **API Benchmarks**: Measure initialization overhead, component creation speed, and high-level API performance
- **Component Benchmarks**: Test individual logger, tracer, and metrics performance under different loads

**Future CLI Tools (v0.2.0):**

```bash
# Will be available as console commands
observe-benchmark-formats
observe-benchmark-api
observe-benchmark-components
```

## Next Steps

Ready to dive deeper? Check out our comprehensive documentation:

📚 **[Full Documentation](https://stride-research.github.io/observe/)**

### Quick Links

- **[Getting Started Guide](docs/getting-started/)** - Installation, configuration, and first steps
- **[API Reference](docs/reference/)** - Complete API documentation
- **[Examples](docs/examples/)** - Working examples for different use cases:
  - [Scientific Computing](docs/examples/scientific_computing_demo.py) - Decorators for computational tasks
  - [Web Services](docs/examples/web_service_scientific_api.py) - FastAPI with observability
  - [HPC Workflows](docs/examples/hpc_cluster_simulation.py) - Large-scale distributed computing
- **[Configuration Guide](docs/getting-started/configuration.md)** - Environment variables and advanced setup
- **[Development Guide](docs/dev/development.md)** - Contributing and development setup

### Current Status: v0.2.0 (Async-First Release)

**Available Now:** Unified async API, environment-aware config, high-performance serialization, CLI tools, comprehensive observability components

**Coming in v0.3.0:** Advanced workflow integrations, enhanced benchmarking tools, observability stack management

## Contributing

We welcome contributions! See our [development guide](docs/dev/development.md) for setup instructions.

```bash
# Quick development setup
git clone https://github.com/stride-research/observe.git
cd observe
pip install -e .[dev]
observe-test-ci  # Run tests locally
```

## License

MIT License - see [LICENSE](LICENSE) file for details.

---

[Stride Research](https://stride-research.org) develops **Observe** for the scientific computing and HPC community. If you use Observe in your research, please cite our work and let us know about your use case!
