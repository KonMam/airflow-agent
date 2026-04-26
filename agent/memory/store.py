"""ChromaDB-backed investigation memory with provider-agnostic embeddings."""

from __future__ import annotations

import chromadb
from chromadb.utils import embedding_functions

from agent.memory.schema import InvestigationRecord
from config import config

COLLECTION_NAME = "investigations"


def _embedding_function():
    if config.embed_model:
        # Use LiteLLM-compatible provider via OpenAI-compatible endpoint
        return embedding_functions.OpenAIEmbeddingFunction(
            api_key=config.llm_api_key,
            model_name=config.embed_model,
        )
    # Fallback: local sentence-transformers (no API key needed)
    return embedding_functions.SentenceTransformerEmbeddingFunction(
        model_name="all-MiniLM-L6-v2"
    )


class InvestigationStore:
    def __init__(self):
        self._client = chromadb.PersistentClient(path=config.chroma_path)
        self._collection = self._client.get_or_create_collection(
            name=COLLECTION_NAME,
            embedding_function=_embedding_function(),
            metadata={"hnsw:space": "cosine"},
        )

    def save(self, record: InvestigationRecord) -> None:
        self._collection.upsert(
            ids=[record.run_id],
            documents=[record.embed_text()],
            metadatas=[record.to_metadata()],
        )

    def query_similar(self, text: str, n_results: int = 3) -> list[dict]:
        """Return up to n_results most similar past investigations."""
        count = self._collection.count()
        if count == 0:
            return []
        results = self._collection.query(
            query_texts=[text],
            n_results=min(n_results, count),
            include=["documents", "metadatas", "distances"],
        )
        similar = []
        for doc, meta, dist in zip(
            results["documents"][0],
            results["metadatas"][0],
            results["distances"][0],
        ):
            similar.append({"document": doc, "metadata": meta, "distance": dist})
        return similar

    def update_outcome(self, run_id: str, outcome: str) -> None:
        results = self._collection.get(ids=[run_id], include=["documents", "metadatas"])
        if not results["ids"]:
            return
        meta = results["metadatas"][0]
        meta["resolution_outcome"] = outcome
        self._collection.update(ids=[run_id], metadatas=[meta])


_store: InvestigationStore | None = None


def get_store() -> InvestigationStore:
    global _store
    if _store is None:
        _store = InvestigationStore()
    return _store
