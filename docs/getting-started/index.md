# Getting Started with RHAPSODY

Welcome to RHAPSODY! This section will guide you through everything you need to know to start using RHAPSODY for your heterogeneous HPC-AI workflows.

## What is RHAPSODY?

RHAPSODY is a high-performance runtime system designed for executing complex workflows that combine traditional HPC simulations with AI/ML workloads. It provides a unified interface for orchestrating tasks across different computational paradigms and computing resources.

## Prerequisites

Before getting started with RHAPSODY, ensure you have:

- **Python 3.9 or higher**
- **Basic familiarity with async/await programming**
- **Understanding of workflow concepts**
- **Access to computational resources** (local machine or HPC cluster)

## Learning Path

Follow this recommended learning path to master RHAPSODY:

### 1. Installation
Start by installing RHAPSODY and its dependencies.

[:octicons-arrow-right-24: Installation Guide](installation.md)

### 2. Quick Start
Learn the basics with a simple "Hello World" workflow.

[:octicons-arrow-right-24: Quick Start Tutorial](quick-start.md)

### 3. First Workflow
Build your first real workflow with multiple tasks.

[:octicons-arrow-right-24: First Workflow](first-workflow.md)

### 4. Configuration
Learn how to configure RHAPSODY for your specific needs.

[:octicons-arrow-right-24: Configuration Guide](configuration.md)

## Key Concepts

Before diving into the tutorials, familiarize yourself with these key concepts:

### Sessions
A session represents a workflow execution context that manages the lifecycle of tasks and resources.

### Backends
Backends are pluggable execution engines that determine how and where your tasks run:
- **Dask**: For local and distributed Python computing
- **RADICAL-Pilot**: For large-scale HPC execution on supercomputers and clusters

### Tasks
Tasks are the fundamental units of computation in RHAPSODY workflows, defined with executables, arguments, and dependencies.

### Callbacks
Callbacks provide real-time monitoring and response to task state changes during workflow execution.

## Getting Help

If you encounter issues or have questions:

- **Documentation**: Browse the comprehensive [User Guide](../user-guide/index.md)
- **Examples**: Check out the [Examples](../examples/index.md) section
- **API Reference**: Consult the detailed [API Reference](../reference/index.md)
- **Issues**: Report bugs or ask questions on [GitHub Issues](https://github.com/radical-cybertools/rhapsody/issues)

## Next Steps

Ready to begin? Start with the [Installation Guide](installation.md) to set up RHAPSODY on your system.

Once you're comfortable with the basics, explore:

- [User Guide](../user-guide/index.md) for comprehensive coverage of RHAPSODY features
- [Examples](../examples/index.md) for real-world use cases
- [Development](../development/index.md) for contributing to RHAPSODY
