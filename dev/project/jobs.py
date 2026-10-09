"""Code of the showcase project, imported inside its Pixi environment as "jobs:<function>"."""

import os
import statistics
import sys


def summarize(values):
    return {
        "label": os.environ.get("SHOWCASE_LABEL", "summary"),
        "n": len(values),
        "mean": statistics.fmean(values),
        "max": max(values),
        "python": sys.version.split()[0],
    }


if __name__ == "__main__":
    values = [float(value) for value in sys.argv[1:]]
    print(f"{len(values)} values, mean {statistics.fmean(values):.3f}")
