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


def fit_normal(values):
    from scipy import stats

    mean, std = stats.norm.fit(values)
    return {"mean": float(mean), "std": float(std), "shapiro_p": float(stats.shapiro(values).pvalue)}


if __name__ == "__main__":
    values = [float(value) for value in sys.argv[1:]]
    print(f"{len(values)} values, mean {statistics.fmean(values):.3f}")
