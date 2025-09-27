# RHAPSODY

![RHAPSODY Logo](assets/rhapsody-banner.png){ align=center }

**RHAPSODY** – **R**untime for **H**eterogeneous **AP**plications, **S**ervice **O**rchestration and **DY**namism

RHAPSODY is a high-performance runtime system designed for executing heterogeneous HPC-AI workflows with dynamic task graphs on high-performance computing infrastructures. It provides seamless integration between different computational paradigms, enabling efficient orchestration of complex scientific workloads.

## Key Features

- **Heterogeneous Execution**: Support for mixed CPU/GPU workloads and diverse computational frameworks
- **Dynamic Task Graphs**: Runtime adaptation of workflow structures based on execution results
- **Multiple Backend Support**: Pluggable execution backends including concurrent, Dask, and RADICAL-Pilot
- **HPC-Optimized**: Designed for large-scale scientific computing on supercomputing clusters
- **AsyncFlow Integration**: Full compatibility with AsyncFlow workflow management
- **Platform Abstraction**: Unified interface across different HPC platforms and resource managers
- **Fault Tolerance**: Robust error handling and recovery mechanisms
- **Real-time Monitoring**: Comprehensive logging and state tracking capabilities

## Quick Start

Get started with RHAPSODY in just a few commands:

```bash
pip install rhapsody
```

```python
import asyncio
import rhapsody
from rhapsody.backends import Session

async def main():
    # Create a session and get backend
    session = Session()
    backend = rhapsody.get_backend("concurrent")

    # Define and execute tasks
    tasks = [
        {"uid": "task_1", "executable": "echo", "arguments": ["Hello, RHAPSODY!"]}
    ]

    await backend.submit_tasks(tasks)

# Run the workflow
asyncio.run(main())
```

[Get Started →](getting-started/index.md){ .md-button .md-button--primary }
[View Examples →](examples/index.md){ .md-button }

## Documentation Sections

<div class="grid cards" markdown>

-   :material-rocket-launch-outline:{ .lg .middle } **Getting Started**

    ---

    Quick installation guide, tutorials, and your first RHAPSODY workflow

    [:octicons-arrow-right-24: Getting Started](getting-started/index.md)

-   :material-book-open-variant:{ .lg .middle } **User Guide**

    ---

    Comprehensive guide covering core concepts, backends, and advanced features

    [:octicons-arrow-right-24: User Guide](user-guide/index.md)

-   :material-code-tags:{ .lg .middle } **Examples**

    ---

    Real-world examples from basic workflows to complex HPC-AI pipelines

    [:octicons-arrow-right-24: Examples](examples/index.md)

-   :material-api:{ .lg .middle } **API Reference**

    ---

    Complete API documentation with detailed function and class references

    [:octicons-arrow-right-24: API Reference](reference/index.md)

</div>

## NSF-Funded Project

RHAPSODY is supported by the National Science Foundation (NSF) under Award ID [2103986](https://www.nsf.gov/awardsearch/showAward?AWD_ID=2103986). This collaborative project aims to advance the state-of-the-art in heterogeneous workflow execution for scientific computing.

[Learn More About the Project →](project/nsf-award.md){ .md-button }

## Contributing

RHAPSODY is an open-source project and we welcome contributions from the community! Whether you're reporting bugs, suggesting features, or contributing code, your help is appreciated.

[Contributing Guide →](development/contributing.md){ .md-button }

## License

RHAPSODY is released under the [MIT License](https://github.com/radical-cybertools/rhapsody/blob/main/LICENSE.md).

---

*Built by the [RADICAL Research Team](https://radical.rutgers.edu/) at Rutgers University*
