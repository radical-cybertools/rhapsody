# Installation

This guide covers installing RHAPSODY and its dependencies on various platforms and environments.

## Requirements

RHAPSODY requires:

- Python 3.9 or higher
- pip package manager
- (Optional) Conda for environment management

## Basic Installation

### Using pip

The simplest way to install RHAPSODY is using pip:

```bash
pip install rhapsody
```

This installs the core RHAPSODY package with the Dask backend enabled by default.

### Using conda

If you use conda for package management:

```bash
conda install -c conda-forge rhapsody
```

## Backend-Specific Installation

RHAPSODY supports two execution backends. Install additional dependencies based on your needs:

### Dask Backend

For local and distributed Python computing (included by default):

```bash
pip install "rhapsody[dask]"
```

### RADICAL-Pilot Backend

For large-scale HPC execution:

```bash
pip install "rhapsody[radical_pilot]"
```

### All Backends

To install all available backends:

```bash
pip install "rhapsody[all]"
```

## Development Installation

If you plan to contribute to RHAPSODY or need the latest development version:

### From Source

```bash
git clone https://github.com/radical-cybertools/rhapsody.git
cd rhapsody
pip install -e .
```

### With Development Dependencies

```bash
git clone https://github.com/radical-cybertools/rhapsody.git
cd rhapsody
pip install -e ".[dev]"
```

This installs additional tools for development, testing, and documentation.

## Virtual Environment Setup

We recommend using a virtual environment to avoid conflicts:

### Using venv

```bash
python -m venv rhapsody-env
source rhapsody-env/bin/activate  # On Windows: rhapsody-env\Scripts\activate
pip install rhapsody
```

### Using conda

```bash
conda create -n rhapsody python=3.9
conda activate rhapsody
pip install rhapsody
```

## Platform-Specific Notes

### Linux

RHAPSODY works on all major Linux distributions. Ensure you have development headers if building from source:

**Ubuntu/Debian:**
```bash
sudo apt-get update
sudo apt-get install python3-dev build-essential
```

**CentOS/RHEL/Fedora:**
```bash
sudo yum install python3-devel gcc gcc-c++
# or on newer versions:
sudo dnf install python3-devel gcc gcc-c++
```

### macOS

Install Xcode command line tools if not already available:

```bash
xcode-select --install
```

Then proceed with pip installation.

### Windows

RHAPSODY supports Windows 10/11. Use PowerShell or Command Prompt:

```cmd
pip install rhapsody
```

For development on Windows, consider using Windows Subsystem for Linux (WSL).

## HPC Environment Setup

### Module-based Systems

Many HPC systems use environment modules:

```bash
module load python/3.9
module load gcc  # If needed for compilation
pip install --user rhapsody
```

### Singularity/Apptainer Containers

Create a container definition file:

```dockerfile
Bootstrap: docker
From: python:3.9-slim

%post
    pip install rhapsody

%environment
    export PATH="/usr/local/bin:$PATH"
```

Build and use the container:

```bash
singularity build rhapsody.sif rhapsody.def
singularity exec rhapsody.sif python your_workflow.py
```

## Verification

Verify your installation by running:

```python
import rhapsody
print(f"RHAPSODY version: {rhapsody.__version__}")

# Test basic functionality
import asyncio
from rhapsody.backends import Session

async def test_install():
    session = Session()
    backend = rhapsody.get_backend("dask")
    print(f"Backend loaded: {backend.__class__.__name__}")

asyncio.run(test_install())
```

Expected output:
```
RHAPSODY version: 0.1.0
Backend loaded: DaskExecutionBackend
```

## Troubleshooting

### Common Issues

**ImportError: No module named 'rhapsody'**
- Ensure you're using the correct Python environment
- Try reinstalling: `pip uninstall rhapsody && pip install rhapsody`

**Permission denied errors**
- Use `pip install --user rhapsody` for user installation
- Or use a virtual environment

**Build failures on HPC systems**
- Load required modules (gcc, python-dev)
- Check if proxy settings are needed for pip

**Backend-specific issues**
- Ensure backend dependencies are installed
- Check system-specific requirements (e.g., MPI for RADICAL-Pilot)

### Getting Help

If you encounter installation issues:

1. Check the [troubleshooting guide](../user-guide/troubleshooting.md)
2. Search existing [GitHub issues](https://github.com/radical-cybertools/rhapsody/issues)
3. Create a new issue with:
   - Operating system and version
   - Python version (`python --version`)
   - Installation command used
   - Complete error message

## Next Steps

Once RHAPSODY is installed, proceed to the [Quick Start](quick-start.md) tutorial to create your first workflow.
