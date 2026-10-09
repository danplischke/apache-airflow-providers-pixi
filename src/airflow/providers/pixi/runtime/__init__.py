"""Code that runs inside the Pixi environment, not on the worker.

The environment may have neither Airflow nor this provider, so these modules use only the standard library and
Python 3.10 or newer. The operators ship their source to the environment; the worker imports them only for their
constants.
"""
