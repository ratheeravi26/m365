from typing import Dict, Optional


def configure_environment(
    env_config: Dict[str, Dict[str, str]],
    widget_defaults: Dict[str, str],
    *,
    default_environment: str = "dev",
) -> Dict[str, str]:
    """
    Register common widgets across notebooks and resolve their values with environment-aware defaults.
    Widget names must match the keys in widget_defaults and in env_config[environment].
    """
    if not env_config:
        raise ValueError("env_config must contain at least one environment entry.")

    available_envs = sorted(env_config.keys())
    if default_environment not in env_config:
        default_environment = available_envs[0]

    dbutils.widgets.dropdown("environment", default_environment, available_envs)
    environment = dbutils.widgets.get("environment")
    env_defaults = env_config.get(environment, {})

    resolved: Dict[str, str] = {"environment": environment}

    for widget_name, fallback in widget_defaults.items():
        widget_default = env_defaults.get(widget_name, fallback)
        dbutils.widgets.text(widget_name, widget_default)
        value = dbutils.widgets.get(widget_name).strip() or widget_default
        resolved[widget_name] = value

    return resolved


def validate_run_mode(run_mode: str, *, allowed: Optional[set] = None) -> str:
    allowed = allowed or {"full", "incremental"}
    normalized = (run_mode or "").lower()
    if normalized not in allowed:
        raise ValueError(f"Unsupported run_mode '{run_mode}'. Allowed values: {sorted(allowed)}.")
    return normalized


def ensure_catalog_schema_pair(catalog_name: str, schema_name: str) -> None:
    catalog_provided = bool(catalog_name.strip())
    schema_provided = bool(schema_name.strip())
    if catalog_provided ^ schema_provided:
        raise ValueError(
            "Both catalog_name and schema_name must be provided (or left blank) to register Delta tables in the metastore."
        )
