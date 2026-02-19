"""
detectors/__init__.py

Detector registry. Each detector module must expose a `detect(df) -> dict | None` function.
Register new detectors here by importing them and adding to REGISTRY.
"""

from detectors.accumulation import detect as accumulation_detect

REGISTRY = {
    "accumulation": accumulation_detect,
    # Future detectors:
    # "distribution": distribution_detect,
    # "breakout": breakout_detect,
    # "orderblock": orderblock_detect,
}


def run_detectors(detector_names: list, df) -> dict:
    """
    Runs all enabled detectors for a pair and returns their results.
    Returns a dict keyed by detector name, value is the result (or None).
    """
    results = {}
    for name in detector_names:
        fn = REGISTRY.get(name)
        if fn is None:
            print(f"[WARN] Detector '{name}' not found in registry.")
            results[name] = None
        else:
            try:
                results[name] = fn(df)
            except Exception as e:
                print(f"[ERROR] Detector '{name}' failed: {e}")
                results[name] = None
    return results
