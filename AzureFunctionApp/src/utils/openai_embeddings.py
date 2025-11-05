import logging
import os
from typing import List, Optional

import numpy as np
from openai import AsyncAzureOpenAI, AzureOpenAI

from src.core.EmbeddingModel import EmbeddingModel


class AzureOpenAIEmbeddingModel(EmbeddingModel):
    """
    Azure OpenAI Embeddings client with sync and async encode methods.

    Mirrors the style of AzureOpenAIGenerator: provides retry logic, structured
    logging, and a simple API for embedding lists of texts.
    """

    def __init__(
        self,
        api_key: str,
        azure_endpoint: str,
        api_version: str,
        deployment: str,
        logger: Optional[logging.Logger] = None,
        default_max_retries: int = 3,
        known_embedding_dimension: Optional[int] = None,
    ) -> None:
        self.client = AzureOpenAI(
            api_version=api_version,
            azure_endpoint=azure_endpoint,
            api_key=api_key,
        )
        self.async_client = AsyncAzureOpenAI(
            api_version=api_version,
            azure_endpoint=azure_endpoint,
            api_key=api_key,
        )
        self.deployment = deployment
        self.logger = logger or logging.getLogger(__name__)
        self.default_max_retries = default_max_retries
        self._embedding_dimension: Optional[int] = known_embedding_dimension

    # ---- EmbeddingModel interface ----
    def get_embedding_dimension(self) -> int:
        if self._embedding_dimension is None:
            self._embedding_dimension = self._fetch_embedding_dimension()
        return self._embedding_dimension

    def count_tokens(self, text: str) -> int:
        # Best-effort estimate without adding tokenizer deps
        return max(1, len(text.split()))

    def encode(self, texts: List[str], show_progress: bool = False) -> np.ndarray:
        return self._embed_sync(texts, max_retries=self.default_max_retries)

    # ---- Async API ----
    async def encode_async(self, texts: List[str]) -> np.ndarray:
        return await self._embed_async(texts, max_retries=self.default_max_retries)

    # ---- Internal helpers ----
    def _embed_sync(self, texts: List[str], max_retries: int) -> np.ndarray:
        if not texts:
            return np.empty((0, 0), dtype=float)

        request_params = {
            "input": texts,
            "model": self.deployment,
        }

        for attempt in range(max_retries):
            try:
                response = self.client.embeddings.create(**request_params)
                embeddings = [item.embedding for item in response.data]

                if embeddings:
                    dim = len(embeddings[0])
                    if self._embedding_dimension is None:
                        self._embedding_dimension = dim
                    self.logger.info(
                        "Embeddings created: count=%s, dim=%s, tokens=%s",
                        len(embeddings),
                        dim,
                        getattr(response.usage, "total_tokens", "?"),
                    )

                return np.array(embeddings, dtype=float)

            except Exception as e:
                self.logger.warning(
                    "Attempt %d/%d (embeddings) failed: %s",
                    attempt + 1,
                    max_retries,
                    str(e),
                )
                if attempt == max_retries - 1:
                    self.logger.error(
                        "All %d attempts failed for embeddings. Last error: %s",
                        max_retries,
                        str(e),
                    )
                    raise

                wait_time = 2 ** attempt
                self.logger.info("Retrying in %d seconds...", wait_time)
                import time as _time

                _time.sleep(wait_time)

        return np.array([], dtype=float)

    async def _embed_async(self, texts: List[str], max_retries: int) -> np.ndarray:
        if not texts:
            return np.empty((0, 0), dtype=float)

        request_params = {
            "input": texts,
            "model": self.deployment,
        }

        for attempt in range(max_retries):
            try:
                response = await self.async_client.embeddings.create(**request_params)  # type: ignore[arg-type]
                embeddings = [item.embedding for item in response.data]

                if embeddings:
                    dim = len(embeddings[0])
                    if self._embedding_dimension is None:
                        self._embedding_dimension = dim
                    self.logger.info(
                        "[async] Embeddings created: count=%s, dim=%s, tokens=%s",
                        len(embeddings),
                        dim,
                        getattr(response.usage, "total_tokens", "?"),
                    )

                return np.array(embeddings, dtype=float)

            except Exception as e:
                self.logger.warning(
                    "[async] Attempt %d/%d (embeddings) failed: %s",
                    attempt + 1,
                    max_retries,
                    str(e),
                )
                if attempt == max_retries - 1:
                    self.logger.error(
                        "[async] All %d attempts failed for embeddings. Last error: %s",
                        max_retries,
                        str(e),
                    )
                    raise

                wait_time = 2 ** attempt
                self.logger.info("[async] Retrying in %d seconds...", wait_time)
                import asyncio as _asyncio

                await _asyncio.sleep(wait_time)

        return np.array([], dtype=float)

    def _fetch_embedding_dimension(self) -> int:
        try:
            resp = self.client.embeddings.create(input=["dim-probe"], model=self.deployment)
            vec = resp.data[0].embedding
            return len(vec)
        except Exception as e:
            self.logger.warning("Failed to probe embedding dimension: %s", str(e))
            # Fallback to a common dimension; callers should not rely on this
            return 1536


if __name__ == "__main__":
    model = AzureOpenAIEmbeddingModel(
        api_key=os.getenv("AZURE_OPENAI_KEY"),
        azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
        api_version=os.getenv("AZURE_OPENAI_API_VERSION"),
        deployment=os.getenv("AZURE_OPENAI_EMBEDDING_DEPLOYMENT"),
    )
    print(model.get_embedding_dimension())
    print(model.count_tokens("Hello, world!"))
    print(model.encode(["Hello, world!"]))