"""Code of the showcase project, imported inside its Pixi environment as "jobs:<function>"."""

import statistics
import sys


def summarize(values):
    return {
        "n": len(values),
        "mean": statistics.fmean(values),
        "max": max(values),
        "python": sys.version.split()[0],
    }
