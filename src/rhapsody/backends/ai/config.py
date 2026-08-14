"""Re-export of dragon.ai.inference types used by RHAPSODY's AI backends.

Single import boundary: RHAPSODY code depends on rhapsody.backends.ai.config,
never on dragon.ai.inference directly. The optional-dependency guard lives
here once instead of being duplicated per consumer.
"""

from __future__ import annotations

try:
    from dragon.ai.inference import (
        BatchingConfig,
        DynamicWorkerConfig,
        GuardrailsConfig,
        HardwareConfig,
        Inference,
        InferenceConfig,
        ModelConfig,
    )
except ImportError:
    BatchingConfig = None
    DynamicWorkerConfig = None
    GuardrailsConfig = None
    HardwareConfig = None
    Inference = None
    InferenceConfig = None
    ModelConfig = None

__all__ = [
    "BatchingConfig",
    "DynamicWorkerConfig",
    "GuardrailsConfig",
    "HardwareConfig",
    "Inference",
    "InferenceConfig",
    "ModelConfig",
]
