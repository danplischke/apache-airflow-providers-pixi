# Environment of the local Airflow of `just standalone`. Source it from the repository root to use the
# Airflow CLI directly: `. dev/airflow-env.sh && uv run airflow dags list`.

# SQLite database, logs and cached inline environments
export AIRFLOW_HOME="$PWD/.airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$PWD/dev/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
# no login in the UI
export AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS=True
# tasks reach the API server through the execution API, so its URL follows the port
export AIRFLOW__API__PORT="${AIRFLOW_PORT:-8080}"
export AIRFLOW__CORE__EXECUTION_API_SERVER_URL="http://localhost:${AIRFLOW__API__PORT}/execution/"
# forked task processes crash on macOS without these
export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES
export no_proxy='*'
