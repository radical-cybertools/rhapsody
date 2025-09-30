# First Workflow Tutorial

In this tutorial, you'll build your first complete RHAPSODY workflow that demonstrates key concepts like task dependencies, data flow, and error handling.

## What We'll Build

We'll create a data processing pipeline that:

1. Generates synthetic data
2. Processes the data through multiple stages
3. Produces a final analysis report
4. Handles errors gracefully

## Prerequisites

Ensure you have:

- RHAPSODY installed with the Dask backend (default installation)
- Python 3.9+ with basic libraries (numpy, pandas - optional)
- A text editor or IDE

## Step-by-Step Tutorial

### Step 1: Project Setup

Create a new directory for your workflow:

```bash
mkdir my_first_rhapsody_workflow
cd my_first_rhapsody_workflow
```

Create the main workflow file `workflow.py`:

```python
import asyncio
import tempfile
import os
import json
import rhapsody
from rhapsody.backends import Session
```

### Step 2: Define the Workflow Structure

Let's create a multi-stage data processing workflow:

```python
async def data_processing_workflow():
    """A complete data processing workflow with multiple stages."""

    # Create session and backend
    session = Session()
    backend = rhapsody.get_backend("dask")

    # Create temporary directory for workflow data
    work_dir = tempfile.mkdtemp()
    print(f"Working directory: {work_dir}")

    # Set up monitoring
    results = []
    def task_monitor(task, state):
        print(f"[{task['uid']}] {state}")
        results.append((task['uid'], state))

    backend.register_callback(task_monitor)
```

### Step 3: Define Tasks with Dependencies

```python
    # Define the workflow tasks
    tasks = [
        # Stage 1: Data Generation
        {
            "uid": "generate_raw_data",
            "executable": "python",
            "arguments": [
                "-c",
                f"""
import random
import json
import os

# Generate synthetic data
data = {{
    'experiment_id': 'EXP001',
    'samples': [
        {{'id': i, 'value': random.uniform(0, 100), 'quality': random.choice(['good', 'fair', 'poor'])}}
        for i in range(100)
    ],
    'metadata': {{'timestamp': '2024-01-01', 'version': '1.0'}}
}}

# Save to file
with open('{work_dir}/raw_data.json', 'w') as f:
    json.dump(data, f, indent=2)

print(f"Generated {{len(data['samples'])}} samples")
                """
            ],
            "output_staging": [f"{work_dir}/raw_data.json"]
        },

        # Stage 2: Data Validation
        {
            "uid": "validate_data",
            "executable": "python",
            "arguments": [
                "-c",
                f"""
import json

# Load and validate data
with open('{work_dir}/raw_data.json', 'r') as f:
    data = json.load(f)

# Validation logic
valid_samples = [s for s in data['samples'] if s['quality'] != 'poor']
validation_report = {{
    'total_samples': len(data['samples']),
    'valid_samples': len(valid_samples),
    'invalid_samples': len(data['samples']) - len(valid_samples),
    'validation_passed': len(valid_samples) > 50
}}

# Save validated data and report
with open('{work_dir}/validated_data.json', 'w') as f:
    json.dump({{'samples': valid_samples, 'metadata': data['metadata']}}, f)

with open('{work_dir}/validation_report.json', 'w') as f:
    json.dump(validation_report, f, indent=2)

print(f"Validation: {{validation_report['valid_samples']}}/{{validation_report['total_samples']}} samples passed")
                """
            ],
            "input_staging": [f"{work_dir}/raw_data.json"],
            "output_staging": [f"{work_dir}/validated_data.json", f"{work_dir}/validation_report.json"],
            "dependencies": ["generate_raw_data"]
        },

        # Stage 3: Statistical Analysis
        {
            "uid": "statistical_analysis",
            "executable": "python",
            "arguments": [
                "-c",
                f"""
import json
import statistics

# Load validated data
with open('{work_dir}/validated_data.json', 'r') as f:
    data = json.load(f)

# Perform statistical analysis
values = [s['value'] for s in data['samples']]
stats = {{
    'count': len(values),
    'mean': statistics.mean(values),
    'median': statistics.median(values),
    'std_dev': statistics.stdev(values) if len(values) > 1 else 0,
    'min': min(values),
    'max': max(values)
}}

# Save analysis results
with open('{work_dir}/statistics.json', 'w') as f:
    json.dump(stats, f, indent=2)

print(f"Analysis complete: mean={{stats['mean']:.2f}}, std={{stats['std_dev']:.2f}}")
                """
            ],
            "input_staging": [f"{work_dir}/validated_data.json"],
            "output_staging": [f"{work_dir}/statistics.json"],
            "dependencies": ["validate_data"]
        },

        # Stage 4: Generate Report
        {
            "uid": "generate_report",
            "executable": "python",
            "arguments": [
                "-c",
                f"""
import json
from datetime import datetime

# Load all previous results
with open('{work_dir}/validation_report.json', 'r') as f:
    validation = json.load(f)

with open('{work_dir}/statistics.json', 'r') as f:
    stats = json.load(f)

# Generate final report
report = f'''
RHAPSODY Workflow Report
========================
Generated: {{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}}

Data Validation
---------------
Total samples: {{validation['total_samples']}}
Valid samples: {{validation['valid_samples']}}
Invalid samples: {{validation['invalid_samples']}}
Validation passed: {{validation['validation_passed']}}

Statistical Analysis
-------------------
Count: {{stats['count']}}
Mean: {{stats['mean']:.3f}}
Median: {{stats['median']:.3f}}
Standard Deviation: {{stats['std_dev']:.3f}}
Range: {{stats['min']:.3f}} - {{stats['max']:.3f}}

Workflow Status: COMPLETED SUCCESSFULLY
'''

# Save report
with open('{work_dir}/final_report.txt', 'w') as f:
    f.write(report)

print("Final report generated successfully")
print(report)
                """
            ],
            "input_staging": [f"{work_dir}/validation_report.json", f"{work_dir}/statistics.json"],
            "output_staging": [f"{work_dir}/final_report.txt"],
            "dependencies": ["statistical_analysis"]
        }
    ]
```

### Step 4: Execute the Workflow

```python
    try:
        print("Starting workflow execution...")
        await backend.submit_tasks(tasks)
        await backend.wait()

        print("\nWorkflow completed successfully!")
        print(f"Results stored in: {work_dir}")

        # Display final report
        with open(f"{work_dir}/final_report.txt", "r") as f:
            print("\n" + "="*50)
            print(f.read())

    except Exception as e:
        print(f"Workflow failed: {e}")
        # Cleanup on failure
        import shutil
        shutil.rmtree(work_dir)
        raise

    return work_dir

# Main execution
if __name__ == "__main__":
    result_dir = asyncio.run(data_processing_workflow())
    print(f"Workflow artifacts saved in: {result_dir}")
```

### Step 5: Run the Complete Workflow

Save all the code above in `workflow.py` and run:

```bash
python workflow.py
```

Expected output:
```
Working directory: /tmp/tmpXXXXXX
Starting workflow execution...
[generate_raw_data] EXECUTING
[generate_raw_data] COMPLETED
Generated 100 samples
[validate_data] EXECUTING
[validate_data] COMPLETED
Validation: 67/100 samples passed
[statistical_analysis] EXECUTING
[statistical_analysis] COMPLETED
Analysis complete: mean=49.23, std=28.91
[generate_report] EXECUTING
[generate_report] COMPLETED
Final report generated successfully

Workflow completed successfully!
Results stored in: /tmp/tmpXXXXXX

==================================================
RHAPSODY Workflow Report
========================
Generated: 2024-01-01 15:30:45

Data Validation
---------------
Total samples: 100
Valid samples: 67
Invalid samples: 33
Validation passed: True

Statistical Analysis
-------------------
Count: 67
Mean: 49.234
Median: 48.567
Standard Deviation: 28.912
Range: 1.234 - 99.876

Workflow Status: COMPLETED SUCCESSFULLY
```

## Key Concepts Demonstrated

### Task Dependencies

```python
"dependencies": ["generate_raw_data"]
```

Tasks wait for their dependencies to complete before starting.

### Data Flow

```python
"input_staging": [f"{work_dir}/raw_data.json"],
"output_staging": [f"{work_dir}/validated_data.json"]
```

Files flow between tasks through the file system.

### Error Handling

```python
try:
    await backend.submit_tasks(tasks)
    await backend.wait()
except Exception as e:
    print(f"Workflow failed: {e}")
```

Proper exception handling ensures graceful failure recovery.

### Real-time Monitoring

```python
def task_monitor(task, state):
    print(f"[{task['uid']}] {state}")
```

Callbacks provide real-time visibility into workflow execution.

## Extending the Workflow

Try these modifications to learn more:

### Add Conditional Logic

```python
# Add a task that only runs if validation passes
{
    "uid": "advanced_analysis",
    "executable": "python",
    "arguments": ["-c", "print('Running advanced analysis...')"],
    "dependencies": ["validate_data"],
    "conditions": {"validation_passed": True}  # Custom condition
}
```

### Add Parallel Processing

```python
# Run multiple analysis tasks in parallel
{
    "uid": "trend_analysis",
    "dependencies": ["validate_data"]
},
{
    "uid": "outlier_detection",
    "dependencies": ["validate_data"]
}
```

### Add Resource Requirements

```python
{
    "uid": "heavy_computation",
    "cpu_cores": 4,
    "memory": "8GB",
    "walltime": "00:30:00"
}
```

## Next Steps

Now that you've built your first workflow, explore:

1. **Different Backends**: Learn about the Dask and RADICAL-Pilot backends in the [API Reference](../reference/backends.md)
2. **Configuration**: Explore [configuration options](configuration.md) for different environments
3. **API Reference**: Dive deeper into the [complete API documentation](../reference/index.md)

Congratulations on completing your first RHAPSODY workflow!
