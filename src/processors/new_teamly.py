import logging
import json
from http import HTTPStatus
from pathlib import Path
from typing import Any

import requests

from src.config import settings
from src.logging_config import setup_logging
from src.processors.base import BaseProcessor
from src.schemas import TeamlyArticle
from src.processors.teamly import clean_text


class TeamlyProcessor(BaseProcessor):
    TEAMLY_SLUG = settings.teamly_api_slug

    def __init__(self, logger: logging.Logger | None = None) -> None:
        super().__init__(logger)
        self._access_token = settings.teamly_api_access_token
        self._refresh_token = settings.teamly_api_refresh_token
        self._client_id = settings.teamly_api_client_id
        self._client_secret = settings.teamly_api_client_secret
        self._excluded_article_ids = settings.teamly_excluded_article_ids_list

        self.authorize_endpoint = (
            f"https://{self.TEAMLY_SLUG}.teamly.ru/api/v1/auth/integration/authorize"
        )
        self.refresh_token_endpoint = (
            f"https://{self.TEAMLY_SLUG}.teamly.ru/api/v1/auth/integration/refresh"
        )
        self.articles_endpoint = f"https://{self.TEAMLY_SLUG}.teamly.ru/api/v1/integrations/space/{settings.teamly_space_id}/tree"
        self.article_detail_endpoint = (
            f"https://{self.TEAMLY_SLUG}.teamly.ru/api/v1/wiki/ql/article"
        )

    @property
    def headers(self) -> dict[str, str]:
        return {
            "Content-Type": "application/json",
            "X-Account-Slug": self.TEAMLY_SLUG,
            "Authorization": f"Bearer {self._access_token}",
        }

    def _persist_env_value(self, key: str, value: str) -> None:
        env_path: Path = settings.env_file
        try:
            if not env_path.exists():
                env_path.write_text(f"{key}={value}\n", encoding="utf-8")
                return
            content = env_path.read_text(encoding="utf-8")
            lines = content.splitlines()
            updated = False
            for idx, line in enumerate(lines):
                if line.strip().startswith(f"{key}="):
                    lines[idx] = f"{key}={value}"
                    updated = True
                    break
            if not updated:
                lines.append(f"{key}={value}")
            env_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        except Exception as exc:
            self.logger.warning(f"Failed to persist {key} to .env: {exc}")

    def _update_tokens_from_response(self, data: dict[str, Any]) -> bool:
        access = (
            data.get("access_token") or data.get("accessToken") or data.get("token")
        )
        refresh = data.get("refresh_token") or data.get("refreshToken")
        if not access:
            return False

        self._access_token = access
        if refresh:
            self._refresh_token = refresh

        self._persist_env_value("TEAMLY_API_ACCESS_TOKEN", self._access_token)
        if refresh:
            self._persist_env_value("TEAMLY_API_REFRESH_TOKEN", self._refresh_token)
        return True

    def refresh_token(self) -> dict[str, Any] | None:
        headers = {
            "Content-Type": "application/json",
            "X-Account-Slug": self.TEAMLY_SLUG,
        }
        response = requests.post(
            url=self.refresh_token_endpoint,
            json={
                "client_id": self._client_id,
                "client_secret": self._client_secret,
                "refresh_token": self._refresh_token,
            },
            headers=headers,
            timeout=30,
        )
        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            self.logger.error(f"Refresh token failed: {exc} | Body: {response.text}")
            return None
        response_json: dict[str, Any] = response.json()
        if not self._update_tokens_from_response(response_json):
            self.logger.error("Refresh token response missing expected fields.")
            return None
        self.logger.info("Teamly tokens refreshed and persisted.")
        return response_json

    def _request(self, method: str, url: str, **kwargs: Any) -> requests.Response:
        headers = kwargs.pop("headers", {}) or {}
        merged_headers = {**self.headers, **headers}
        timeout = kwargs.pop("timeout", 30)

        response = requests.request(
            method=method, url=url, headers=merged_headers, timeout=timeout, **kwargs
        )
        if response.status_code in (HTTPStatus.UNAUTHORIZED, HTTPStatus.FORBIDDEN):
            self.logger.info(
                "Access token likely expired. Attempting to refresh and retry..."
            )
            if self.refresh_token() is not None:
                merged_headers = {**self.headers, **headers}
                response = requests.request(
                    method=method,
                    url=url,
                    headers=merged_headers,
                    timeout=timeout,
                    **kwargs,
                )
        return response

    def list_articles_page(
        self, page: int = 1
    ) -> tuple[list[TeamlyArticle], dict[str, Any]]:
        self.logger.info(
            f"Fetching Teamly articles page={page} for space {settings.teamly_space_id}"
        )
        response = self._request(
            "GET",
            self.articles_endpoint,
            params={"page": page},
        )
        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            self.logger.error(
                f"Failed to fetch articles page {page}: {exc} | Body: {response.text}"
            )
            raise
        data = response.json() or {}
        items = data.get("items") or []
        pagination = data.get("pagination") or {}
        excluded = set(self._excluded_article_ids)
        if excluded:
            before = len(items)
            items = [it for it in items if str(it.get("id")) not in excluded]
            removed = before - len(items)
            if removed:
                self.logger.info(f"Excluded {removed} items on page {page}")
        parsed = [TeamlyArticle.model_validate(obj) for obj in items]
        self.logger.info(
            f"Fetched {len(parsed)} items on page {pagination.get('currentPage', page)} / {pagination.get('lastPage', '?')}"
        )
        return parsed, pagination

    def list_all_articles(self) -> list[TeamlyArticle]:
        page = 1
        all_items: list[TeamlyArticle] = []
        last_page: int | None = None
        while last_page is None or page <= last_page:
            items, pagination = self.list_articles_page(page=page)
            all_items.extend(items)
            current = int(pagination.get("currentPage", page) or page)
            last_page = int(pagination.get("lastPage", current) or current)
            self.logger.info(
                f"Accumulated articles: {len(all_items)} after page {current} of {last_page}"
            )
            if page >= last_page:
                break
            page += 1
        self.logger.info(f"Total articles collected: {len(all_items)}")
        return all_items

    def _extract_text_from_editor_content(self, content_field: Any) -> str:
        try:
            if isinstance(content_field, str):
                obj = json.loads(content_field)
            elif isinstance(content_field, dict):
                obj = content_field
            else:
                return ""

            parts: list[str] = []

            def visit(node: Any) -> None:
                if isinstance(node, dict):
                    text_val = node.get("text")
                    if isinstance(text_val, str):
                        parts.append(text_val)
                    for key in ("content", "children", "items", "paragraphs"):
                        child = node.get(key)
                        if isinstance(child, list):
                            for c in child:
                                visit(c)
                elif isinstance(node, list):
                    for item in node:
                        visit(item)

            visit(obj)
            combined = "\n".join(s for s in (p.strip() for p in parts) if s)
            return clean_text(combined)
        except Exception as exc:
            self.logger.warning(f"Failed to parse editor content: {exc}")
            return ""

    def get_article_details(self, article_id: str) -> dict[str, Any]:
        payload = {
            "query": {
                "__filter": {
                    "id": article_id,
                }
            }
        }
        self.logger.info(f"Fetching Teamly article details for id={article_id}")
        response = self._request("POST", self.article_detail_endpoint, json=payload)
        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            self.logger.error(
                f"Failed to fetch article {article_id}: {exc} | Body: {response.text}"
            )
            raise
        data = response.json() or {}
        self.logger.info(f"Fetched details for id={article_id}")
        return data

    def get_article_clean_text(self, article_id: str) -> str:
        data = self.get_article_details(article_id)
        editor_obj = (data or {}).get("editorContentObject") or {}
        content_field = editor_obj.get("content")
        text = self._extract_text_from_editor_content(content_field)
        if not text:
            latest = (data or {}).get("latestProperties") or {}
            title = ((latest.get("title") or {}).get("text")) or data.get("title")
            text = title or ""
        cleaned = clean_text(text)
        self.logger.info(
            f"Produced cleaned text for id={article_id}, length={len(cleaned)} chars"
        )
        return cleaned


if __name__ == "__main__":
    setup_logging()
    proc = TeamlyProcessor()
    all_articles = proc.list_all_articles()
    proc.logger.info(f"Total articles found: {len(all_articles)}")
