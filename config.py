import os

from dotenv import load_dotenv

load_dotenv()


class Config:
    # LLM
    llm_model: str = os.getenv("LLM_MODEL", "openai/gpt-4o")
    llm_api_key: str = os.getenv("LLM_API_KEY", "")
    llm_max_tokens: int = int(os.getenv("LLM_MAX_TOKENS", "1024"))
    # Comma-separated fallback models tried in order when primary is rate-limited > llm_fallback_threshold_s
    llm_fallback_models: list[str] = [
        m.strip() for m in os.getenv("LLM_FALLBACK_MODELS", "").split(",") if m.strip()
    ]
    llm_fallback_threshold_s: int = int(os.getenv("LLM_FALLBACK_THRESHOLD_S", "60"))
    embed_model: str = os.getenv(
        "EMBED_MODEL", ""
    )  # empty = use local sentence-transformers

    # Airflow
    airflow_url: str = os.getenv("AIRFLOW_URL", "http://localhost:8080")
    airflow_api_version: str = os.getenv(
        "AIRFLOW_API_VERSION", "v1"
    )  # v1=Airflow2, v2=Airflow3
    airflow_user: str = os.getenv("AIRFLOW_USER", "airflow")
    airflow_password: str = os.getenv("AIRFLOW_PASSWORD", "airflow")
    airflow_api_token: str = os.getenv(
        "AIRFLOW_API_TOKEN", ""
    )  # Bearer token; takes precedence over user/password

    # Poller
    poll_interval: int = int(os.getenv("POLL_INTERVAL", "30"))
    poll_dag_ids: list[str] = [
        d for d in os.getenv("POLL_DAG_IDS", "").split(",") if d
    ]  # empty = poll all DAGs

    # Memory
    chroma_path: str = os.getenv("CHROMA_PATH", ".chroma")
    db_path: str = os.getenv("DB_PATH", ".airflow_agent.db")
    langgraph_db_path: str = os.getenv("LANGGRAPH_DB_PATH", ".langgraph.db")
    db_retention_days: int = int(os.getenv("DB_RETENTION_DAYS", "90"))

    @property
    def api_prefix(self) -> str:
        return f"/api/{self.airflow_api_version}"


config = Config()
