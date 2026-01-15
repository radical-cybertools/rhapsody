# Backend API Reference

API documentation for RHAPSODY execution backends.

## Base Backend Interface

::: rhapsody.backends.base.BaseBackend
    options:
      show_source: false
      show_root_heading: true

## Dask Execution Backend

::: rhapsody.backends.execution.dask_parallel.DaskExecutionBackend
    options:
      show_source: false
      show_root_heading: true

## RADICAL-Pilot Execution Backend

::: rhapsody.backends.execution.radical_pilot.RadicalExecutionBackend
    options:
      show_source: false
      show_root_heading: true

## Backend Discovery

::: rhapsody.backends.discovery.discover_backends
    options:
      show_source: false

::: rhapsody.backends.discovery.get_backend
    options:
      show_source: false

## Backend Registry

::: rhapsody.backends.discovery.BackendRegistry
    options:
      show_source: false
