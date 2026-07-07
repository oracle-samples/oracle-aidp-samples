"""
SizeWise — AIDP pricing primitives.

Self-contained port of the AIDPPricing / AIDPCluster classes from the original
`generate_aidp_capacity_reports_v4.py` engine, so the tool has no external file
dependency. The cost math is REUSED verbatim (do not diverge from these rates —
they mirror Oracle's published AIDP Unit pricing).

AIDP Unit (AIDPU) rates, per hour:
    ARM   OCPU : 27 AIDPU     AMD OCPU : 67 AIDPU     Intel OCPU : 87 AIDPU
    NVIDIA GPU : 4110 AIDPU   Memory   : 3 AIDPU/GB
    1 AIDPU = $0.001 USD
    Default Master Catalog compute bills a minimum of 230 AIDPU/hr.
    Object Storage (standard/hot tier): $0.0255 /GB-month.
"""

from __future__ import annotations

DAYS_PER_MONTH = 30


class AIDPPricing:
    """AIDP pricing constants + node cost calculator (from Oracle documentation)."""

    # AIDPU rates per hour
    ARM_OCPU = 27
    AMD_OCPU = 67
    INTEL_OCPU = 87
    NVIDIA_GPU = 4110
    MEMORY_GB = 3
    AIDPU_TO_USD = 0.001

    # Platform / storage
    PLATFORM_MIN_AIDPU = 230.0          # Default Master Catalog billed floor
    OBJECT_STORAGE_GB_MONTH = 0.0255    # standard/hot tier $/GB-month
    ARCHIVE_GB_MONTH = 0.0026           # archive tier $/GB-month

    _PROC_RATES = {"ARM": ARM_OCPU, "AMD": AMD_OCPU, "Intel": INTEL_OCPU}

    @classmethod
    def proc_rate(cls, processor: str) -> int:
        """AIDPU/OCPU-hr for a processor family (defaults to ARM if unknown)."""
        return cls._PROC_RATES.get(processor, cls.ARM_OCPU)

    @classmethod
    def node_aidpu_per_hour(cls, ocpu, memory_gb, processor="ARM", gpu=0) -> float:
        """AIDPU/hr for a single node of the given shape."""
        return (
            ocpu * cls.proc_rate(processor)
            + memory_gb * cls.MEMORY_GB
            + gpu * cls.NVIDIA_GPU
        )

    @classmethod
    def node_usd_per_hour(cls, ocpu, memory_gb, processor="ARM", gpu=0) -> float:
        """USD/hr for a single node of the given shape."""
        return cls.node_aidpu_per_hour(ocpu, memory_gb, processor, gpu) * cls.AIDPU_TO_USD

    @classmethod
    def calculate_compute_cost(cls, ocpu, memory_gb, processor="ARM", gpu=0) -> dict:
        """Return the cost breakdown for a single node (mirrors the base engine)."""
        aidpu_per_hour = cls.node_aidpu_per_hour(ocpu, memory_gb, processor, gpu)
        cost_per_hour = aidpu_per_hour * cls.AIDPU_TO_USD
        return {
            "aidpu_per_hour": aidpu_per_hour,
            "cost_per_hour": cost_per_hour,
            "cost_per_day_12h": cost_per_hour * 12,
            "cost_per_month_24x7": cost_per_hour * 24 * DAYS_PER_MONTH,
        }


class AIDPCluster:
    """
    AIDP autoscale cluster (driver + autoscaling executors).

    Cost model (reused verbatim from the base engine):
        avg_executors = min_exec + (max_exec - min_exec) * avg_utilization
        cluster_$/hr  = driver_$/hr + executor_$/hr * avg_executors
        monthly_$     = cluster_$/hr * hours_per_day * 30
    avg_executors is intentionally NOT rounded, matching the base engine.
    """

    def __init__(self, driver_ocpu, driver_memory, executor_ocpu, executor_memory,
                 min_executors, max_executors, processor="ARM", hours_per_day=12,
                 avg_utilization=0.6, executor_gpu=0):
        self.driver_ocpu = driver_ocpu
        self.driver_memory = driver_memory
        self.executor_ocpu = executor_ocpu
        self.executor_memory = executor_memory
        self.executor_gpu = executor_gpu
        self.min_executors = min_executors
        self.max_executors = max_executors
        self.processor = processor
        self.hours_per_day = hours_per_day
        self.avg_utilization = avg_utilization

        self.avg_executors = min_executors + (max_executors - min_executors) * avg_utilization
        self._calculate_costs()

    def _calculate_costs(self):
        self.driver_cost_per_hour = AIDPPricing.node_usd_per_hour(
            self.driver_ocpu, self.driver_memory, self.processor
        )
        self.executor_cost_per_hour = AIDPPricing.node_usd_per_hour(
            self.executor_ocpu, self.executor_memory, self.processor, self.executor_gpu
        )
        self.total_cost_per_hour = (
            self.driver_cost_per_hour + self.executor_cost_per_hour * self.avg_executors
        )
        self.daily_cost = self.total_cost_per_hour * self.hours_per_day
        self.monthly_cost = self.daily_cost * DAYS_PER_MONTH

        # Peak (max) and average provisioned resource totals
        self.avg_total_ocpu = self.driver_ocpu + self.executor_ocpu * self.avg_executors
        self.avg_total_memory = self.driver_memory + self.executor_memory * self.avg_executors
        self.max_total_ocpu = self.driver_ocpu + self.executor_ocpu * self.max_executors
        self.max_total_memory = self.driver_memory + self.executor_memory * self.max_executors
        self.max_total_gpu = self.executor_gpu * self.max_executors
