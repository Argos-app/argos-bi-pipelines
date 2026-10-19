"""Entradas das tasks, seguindo argos/delta_layers.py do relacional."""
from .silver import run_silver
from .gold import run_gold, validate_pipeline

__all__ = ['run_silver', 'run_gold', 'validate_pipeline']
