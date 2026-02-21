"""
detectors/__init__.py

Detector registry. Each detector module must expose a `detect(df, **kwargs) -> dict | None` function.
Register new detectors here by importing them and adding to REGISTRY.
"""

from detectors.accumulation import detect as accumulation_detect
from detectors.supply_demand import detect as supply_demand_detect
from detectors.fvg import detect as fvg_detect

REGISTRY = {
    "accumulation":  accumulation_detect,
    "supply_demand": supply_demand_detect,
    "fvg": fvg_detect,
}


def run_detectors(detector_names: list, df, detector_params: dict = None) -> dict:
    if detector_params is None:
        detector_params = {}

    results = {}
    for name in detector_names:
        fn = REGISTRY.get(name)
        if fn is None:
            print(f"[WARN] Detector '{name}' not found in registry.")
            results[name] = None
        else:
            try:
                params = detector_params.get(name, {})
                results[name] = fn(df, **params)
            except Exception as e:
                print(f"[ERROR] Detector '{name}' failed: {e}")
                results[name] = None
    return results
