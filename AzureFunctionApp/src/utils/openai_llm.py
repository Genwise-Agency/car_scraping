# flake8: noqa: E501
import logging
import time
from typing import Optional, Type

from openai import AsyncAzureOpenAI, AzureOpenAI
from pydantic import BaseModel

from src.core.Generator import Generator


class AzureOpenAIGenerator(Generator):
    """
    Azure OpenAI client wrapper with retry mechanism and structured response
    support. Provides both sync and async generation methods.
    """

    def __init__(
        self,
        api_key: str,
        azure_endpoint: str,
        api_version: str,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        """
        Initialize Azure OpenAI client

        Args:
            api_key: Azure OpenAI API key
            azure_endpoint: Azure OpenAI endpoint URL
            api_version: API version to use
            logger: Optional logger instance, creates default if None
        """
        self.client = AzureOpenAI(
            api_version=api_version,
            azure_endpoint=azure_endpoint,
            api_key=api_key,
        )
        # Async client for concurrency
        self.async_client = AsyncAzureOpenAI(
            api_version=api_version,
            azure_endpoint=azure_endpoint,
            api_key=api_key,
        )
        self.logger = logger or logging.getLogger(__name__)

    def generate(
        self,
        model_deployment: str,
        prompt: str,
        max_retries: int = 3,
        response_schema: Optional[Type[BaseModel]] = None,
        system_prompt: Optional[str] = None,
        max_tokens: int = 10000,
        temperature: float = 0,
        top_p: float = 1.0,
        default_system_prompt: str = "You are a helpful assistant."
    ) -> str:
        """
        Make a chat completion call to Azure OpenAI with retry mechanism

        Args:
            model_deployment: Name of the deployed model
            prompt: User prompt/message
            max_retries: Maximum number of retry attempts
            response_schema: Optional Pydantic model for structured JSON response
            system_prompt: Optional system prompt (overrides default)
            max_tokens: Maximum tokens in response
            temperature: Sampling temperature (0-2)
            top_p: Nucleus sampling parameter (0-1)
            default_system_prompt: Default system prompt if none provided

        Returns:
            Response content as string

        Raises:
            Exception: If all retry attempts fail
        """
        if system_prompt is None:
            system_prompt = default_system_prompt

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": prompt},
        ]

        request_params = {
            "messages": messages,
            "max_tokens": max_tokens,
            "temperature": temperature,
            "top_p": top_p,
            "model": model_deployment,
        }

        if response_schema:
            request_params["response_format"] = {
                "type": "json_schema",
                "json_schema": {
                    "name": response_schema.__name__,
                    "schema": response_schema.model_json_schema(),
                },
            }

        # Retry mechanism
        for attempt in range(max_retries):
            try:
                response = self.client.chat.completions.create(**request_params)

                # Log token usage
                input_len = len(request_params["messages"][1]["content"])  # type: ignore[index]
                output_text = response.choices[0].message.content
                output_len = len(output_text) if output_text else 0
                self.logger.debug("Input message length: %d chars", input_len)
                self.logger.debug("Output message length: %d chars", output_len)
                self.logger.info("Token usage - Total: %s, Prompt: %s, Completion: %s",
                                 getattr(response.usage, "total_tokens", "?"),
                                 getattr(response.usage, "prompt_tokens", "?"),
                                 getattr(response.usage, "completion_tokens", "?"))

                return response.choices[0].message.content or ""

            except Exception as e:
                self.logger.warning("Attempt %d/%d failed: %s", attempt + 1, max_retries, str(e))
                if attempt == max_retries - 1:
                    self.logger.error(
                        "All %d attempts failed. Last error: %s",
                        max_retries,
                        str(e),
                    )
                    raise

                # Exponential backoff: wait 2^attempt seconds
                wait_time = 2 ** attempt
                self.logger.info("Retrying in %d seconds...", wait_time)
                time.sleep(wait_time)

    async def generate_async(
        self,
        model_deployment: str,
        prompt: str,
        max_retries: int = 3,
        response_schema: Optional[Type[BaseModel]] = None,
        system_prompt: Optional[str] = None,
        max_tokens: int = 10000,
        temperature: float = 0,
        top_p: float = 1.0,
        default_system_prompt: str = "You are a helpful assistant.",
    ) -> str:
        """
        Async version of generate() using AsyncAzureOpenAI.

        Returns:
            Response content as string
        """
        if system_prompt is None:
            system_prompt = default_system_prompt

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": prompt},
        ]

        request_params = {
            "messages": messages,
            "max_tokens": max_tokens,
            "temperature": temperature,
            "top_p": top_p,
            "model": model_deployment,
        }

        if response_schema:
            request_params["response_format"] = {
                "type": "json_schema",
                "json_schema": {
                    "name": response_schema.__name__,
                    "schema": response_schema.model_json_schema(),
                },
            }

        for attempt in range(max_retries):
            try:
                response = await self.async_client.chat.completions.create(  # type: ignore[arg-type]
                    **request_params
                )

                input_len = len(request_params["messages"][1]["content"])  # type: ignore[index]
                output_text = response.choices[0].message.content
                output_len = len(output_text) if output_text else 0
                self.logger.debug("[async] Input message length: %d chars", input_len)
                self.logger.debug("[async] Output message length: %d chars", output_len)
                self.logger.info("[async] Token usage - Total: %s, Prompt: %s, Completion: %s",
                                 getattr(response.usage, "total_tokens", "?"),
                                 getattr(response.usage, "prompt_tokens", "?"),
                                 getattr(response.usage, "completion_tokens", "?"))

                return response.choices[0].message.content or ""

            except Exception as e:
                self.logger.warning("[async] Attempt %d/%d failed: %s", attempt + 1, max_retries, str(e))
                if attempt == max_retries - 1:
                    self.logger.error(
                        "[async] All %d attempts failed. Last error: %s",
                        max_retries,
                        str(e),
                    )
                    raise

                # Exponential backoff: wait 2^attempt seconds
                wait_time = 2 ** attempt
                self.logger.info("[async] Retrying in %d seconds...", wait_time)
                # Use asyncio.sleep via time.sleep in sync context is not allowed here
                import asyncio as _asyncio  # local import to avoid global dependency

                await _asyncio.sleep(wait_time)
