"""Bronze -> silver -> gold transforms.

* :mod:`gold` — business marts exposed through content packs.
* Future: ``silver`` namespace for typing/projection helpers shared across
  silver dim builds once duplication appears.
"""

from . import gold

__all__ = ["gold"]
