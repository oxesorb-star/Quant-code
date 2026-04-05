import json
import re
from typing import Callable, Optional

import httpx

from qt_config import (
    MODEL_GEMMA,
    MODEL_R1,
    OLLAMA_API,
    get_env_optional,
)


OPENROUTER_API_KEY = get_env_optional("OPENROUTER_API_KEY", "")
OPENROUTER_BASE_URL = get_env_optional("OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1")

AI_PROVIDER_SCREEN = get_env_optional("AI_PROVIDER_SCREEN", "ollama").strip().lower()
AI_PROVIDER_DEEP_AUDIT = get_env_optional("AI_PROVIDER_DEEP_AUDIT", "ollama").strip().lower()
AI_PROVIDER_WATCH_CONFIRM = get_env_optional("AI_PROVIDER_WATCH_CONFIRM", "openrouter").strip().lower()
AI_PROVIDER_EXEC_GATE = get_env_optional("AI_PROVIDER_EXEC_GATE", "openrouter").strip().lower()

MODEL_SCREEN = get_env_optional("MODEL_SCREEN", MODEL_GEMMA)
MODEL_DEEP_AUDIT = get_env_optional("MODEL_DEEP_AUDIT", MODEL_R1)
MODEL_WATCH_CONFIRM = get_env_optional("MODEL_WATCH_CONFIRM", "google/gemini-3.1-flash-lite-preview")
MODEL_EXEC_GATE = get_env_optional("MODEL_EXEC_GATE", "openai/gpt-5.4-mini")

_STAGE_PROVIDER_MAP = {
    "screen": AI_PROVIDER_SCREEN,
    "deep_audit": AI_PROVIDER_DEEP_AUDIT,
    "watch_confirm": AI_PROVIDER_WATCH_CONFIRM,
    "execution_gate": AI_PROVIDER_EXEC_GATE,
}

_STAGE_MODEL_MAP = {
    "screen": MODEL_SCREEN,
    "deep_audit": MODEL_DEEP_AUDIT,
    "watch_confirm": MODEL_WATCH_CONFIRM,
    "execution_gate": MODEL_EXEC_GATE,
}


def _extract_json_string(text: str) -> Optional[str]:
    match = re.search(r"(\{.*\}|\[.*\])", text or "", re.DOTALL)
    if not match:
        return None
    candidate = match.group(0)
    json.loads(candidate)
    return candidate


def resolve_stage_provider(stage: str, provider: Optional[str] = None) -> str:
    resolved = (provider or _STAGE_PROVIDER_MAP.get(stage, "ollama")).strip().lower()
    if resolved not in ("ollama", "openrouter"):
        raise ValueError(f"Unsupported AI provider: {resolved}")
    return resolved


def resolve_stage_model(stage: str, model: Optional[str] = None) -> str:
    resolved = (model or _STAGE_MODEL_MAP.get(stage, "")).strip()
    if not resolved:
        raise ValueError(f"Missing model for AI stage: {stage}")
    return resolved


async def ask_ollama(
    model: str,
    prompt: str,
    *,
    temperature: float = 0.2,
    force_json: bool = False,
    timeout: float = 90.0,
) -> Optional[str]:
    payload = {
        "model": model,
        "prompt": prompt,
        "stream": False,
        "options": {"temperature": temperature, "num_ctx": 4096},
    }
    async with httpx.AsyncClient(timeout=timeout, trust_env=False, follow_redirects=True) as client:
        resp = await client.post(OLLAMA_API, json=payload)
        resp.raise_for_status()
        full_response = resp.json().get("response", "").strip()
        if not full_response:
            return None
        if not force_json:
            return full_response
        return _extract_json_string(full_response)


async def ask_openrouter(
    model: str,
    prompt: str,
    *,
    system_prompt: Optional[str] = None,
    temperature: float = 0.2,
    force_json: bool = False,
    timeout: float = 90.0,
) -> Optional[str]:
    if not OPENROUTER_API_KEY:
        raise RuntimeError("Missing required environment variable: OPENROUTER_API_KEY")

    messages = []
    if system_prompt:
        messages.append({"role": "system", "content": system_prompt})
    messages.append({"role": "user", "content": prompt})

    payload = {
        "model": model,
        "messages": messages,
        "temperature": temperature,
    }
    if force_json:
        payload["response_format"] = {"type": "json_object"}

    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type": "application/json",
    }

    async with httpx.AsyncClient(timeout=timeout, trust_env=False, follow_redirects=True) as client:
        resp = await client.post(f"{OPENROUTER_BASE_URL.rstrip('/')}/chat/completions", headers=headers, json=payload)
        resp.raise_for_status()
        data = resp.json()
        choices = data.get("choices") or []
        if not choices:
            return None
        message = choices[0].get("message") or {}
        content = (message.get("content") or "").strip()
        if not content:
            return None
        if not force_json:
            return content
        return _extract_json_string(content)


async def ask_ai(
    *,
    stage: str,
    prompt: str,
    provider: Optional[str] = None,
    model: Optional[str] = None,
    system_prompt: Optional[str] = None,
    temperature: float = 0.2,
    force_json: bool = False,
    timeout: float = 90.0,
    log_terminal_fn: Optional[Callable[[str, str], None]] = None,
    logger_instance=None,
) -> Optional[str]:
    resolved_provider = resolve_stage_provider(stage, provider)
    resolved_model = resolve_stage_model(stage, model)

    try:
        if resolved_provider == "ollama":
            return await ask_ollama(
                resolved_model,
                prompt,
                temperature=temperature,
                force_json=force_json,
                timeout=timeout,
            )
        return await ask_openrouter(
            resolved_model,
            prompt,
            system_prompt=system_prompt,
            temperature=temperature,
            force_json=force_json,
            timeout=timeout,
        )
    except httpx.TimeoutException as e:
        if log_terminal_fn:
            log_terminal_fn("AI 超时", f"{stage} | 模型 {resolved_model} 在 {timeout:.0f}s 内未完成响应")
        if logger_instance:
            logger_instance.error(f"AI gateway timeout [{stage}]: {e}")
    except httpx.ConnectError as e:
        if log_terminal_fn:
            if resolved_provider == "ollama":
                log_terminal_fn("AI 链路断开", f"{stage} | 无法连接本地 Ollama 服务")
            else:
                log_terminal_fn("AI 链路断开", f"{stage} | 无法连接 OpenRouter")
        if logger_instance:
            logger_instance.error(f"AI gateway connect error [{stage}]: {e}")
    except httpx.HTTPStatusError as e:
        if log_terminal_fn:
            log_terminal_fn("AI HTTP 错误", f"{stage} | 模型 {resolved_model} 返回 {e.response.status_code}")
        if logger_instance:
            logger_instance.error(f"AI gateway http error [{stage}]: {e} | body={e.response.text[:200]}")
    except json.JSONDecodeError as e:
        if log_terminal_fn:
            log_terminal_fn("AI JSON 解析失败", f"{stage} | 模型 {resolved_model} 返回内容不是合法 JSON")
        if logger_instance:
            logger_instance.error(f"AI gateway json parse error [{stage}]: {e}")
    except Exception as e:
        if log_terminal_fn:
            log_terminal_fn("AI 未知错误", f"{stage} | {type(e).__name__}: {e}")
        if logger_instance:
            logger_instance.error(f"AI gateway unexpected error [{stage}]", exc_info=True)
    return None
