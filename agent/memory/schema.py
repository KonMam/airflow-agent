from dataclasses import dataclass, field
from datetime import datetime, timezone


@dataclass
class InvestigationRecord:
    dag_id: str
    run_id: str
    task_id: str  # primary failing task
    error_summary: str  # short error message extracted from logs
    diagnosis: str
    recommended_action: str
    airflow_version: str
    timestamp: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    resolution_outcome: str = "unknown"  # resolved | unresolved | unknown

    def embed_text(self) -> str:
        """Text used for embedding — what the similarity search matches on."""
        return f"{self.dag_id} {self.task_id} {self.error_summary} {self.diagnosis}"

    def to_metadata(self) -> dict:
        return {
            "dag_id": self.dag_id,
            "run_id": self.run_id,
            "task_id": self.task_id,
            "recommended_action": self.recommended_action,
            "airflow_version": self.airflow_version,
            "timestamp": self.timestamp,
            "resolution_outcome": self.resolution_outcome,
        }
