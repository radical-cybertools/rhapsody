# RHAPSODY on NSF and DOE Machines

This section provides machine-specific setup guides and verified examples for running RHAPSODY on NSF and DOE leadership computing facilities. These machines are periodically tested and verified
against RHAPSODY and it pre-release changes.

Each guide covers environment setup, module loading, job allocation, and working examples — with different examples.

## Supported Machines

| Machine | Facility | Stack | Backend | Status |
|---------|----------|-------|---------|--------|
| [NCSA Delta](ncsa-delta.md) | NSF / NCSA | Cray MPICH (PMIx) | `DragonExecutionBackendV3` | Verified |
| [NERSC Perlmutter](perlmutter.md) | DOE / NERSC | Cray libfabric + CUDA | `DragonVllmInferenceBackend` | Verified |

More machines will be added as they are tested and validated.
