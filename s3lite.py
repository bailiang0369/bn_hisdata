"""
Minimal, dependency-free (requests only) S3 client for reading the dataset bucket.

This is a self-contained copy of the client used by cryptohftdata's bulk
download to talk to R2 directly (ListObjectsV2 + GetObject, both signed
bodyless GETs). Copied from the SDK's s3lite module so the project runs with
no SDK dependency. Public surface mirrors the slice of boto3 that the
downloader uses.
"""

from __future__ import annotations

import hashlib
import hmac
import time
import xml.etree.ElementTree as ET  # nosec B405 - see _parse_xml for hardening
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, List, Optional
from urllib.parse import quote, urlparse

import requests
from requests.adapters import HTTPAdapter

logger = __import__("logging").getLogger(__name__)

ALGORITHM = "AWS4-HMAC-SHA256"
SERVICE = "s3"

EMPTY_PAYLOAD_SHA256 = (
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
)

UNRESERVED_PATH = "/-_.~"
UNRESERVED_QUERY = "-_.~"

LIST_NAMESPACE = "{http://s3.amazonaws.com/doc/2006-03-01/}"

RETRY_STATUSES = frozenset({429, 500, 502, 503, 504})


class S3LiteError(Exception):
    def __init__(self, code: str, message: str, status: Optional[int] = None):
        super().__init__(
            f"An error occurred ({code}) when calling the S3 operation: {message}"
        )
        self.response: Dict[str, Any] = {
            "Error": {"Code": code, "Message": message},
            "ResponseMetadata": {"HTTPStatusCode": status},
        }


def _sign(key: bytes, message: str) -> bytes:
    return hmac.new(key, message.encode("utf-8"), hashlib.sha256).digest()


def _signing_key(secret: str, date_stamp: str, region: str) -> bytes:
    key = _sign(f"AWS4{secret}".encode("utf-8"), date_stamp)
    key = _sign(key, region)
    key = _sign(key, SERVICE)
    return _sign(key, "aws4_request")


def canonical_query_string(params: Dict[str, str]) -> str:
    encoded = [
        (quote(str(key), safe=UNRESERVED_QUERY), quote(str(value), safe=UNRESERVED_QUERY))
        for key, value in params.items()
    ]
    encoded.sort()
    return "&".join(f"{key}={value}" for key, value in encoded)


def canonical_uri(path: str) -> str:
    return quote(path, safe=UNRESERVED_PATH)


def build_authorization(
    *,
    method: str,
    host: str,
    path: str,
    params: Dict[str, str],
    access_key: str,
    secret_key: str,
    session_token: Optional[str],
    region: str,
    now: datetime,
) -> Dict[str, str]:
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    date_stamp = now.strftime("%Y%m%d")

    headers = {
        "host": host,
        "x-amz-content-sha256": EMPTY_PAYLOAD_SHA256,
        "x-amz-date": amz_date,
    }
    if session_token:
        headers["x-amz-security-token"] = session_token

    signed_headers = ";".join(sorted(headers))
    canonical_headers = "".join(
        f"{name}:{headers[name].strip()}\n" for name in sorted(headers)
    )

    canonical_request = "\n".join(
        [
            method,
            canonical_uri(path),
            canonical_query_string(params),
            canonical_headers,
            signed_headers,
            EMPTY_PAYLOAD_SHA256,
        ]
    )

    scope = f"{date_stamp}/{region}/{SERVICE}/aws4_request"
    string_to_sign = "\n".join(
        [
            ALGORITHM,
            amz_date,
            scope,
            hashlib.sha256(canonical_request.encode("utf-8")).hexdigest(),
        ]
    )

    signature = hmac.new(
        _signing_key(secret_key, date_stamp, region),
        string_to_sign.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()

    headers["Authorization"] = (
        f"{ALGORITHM} Credential={access_key}/{scope}, "
        f"SignedHeaders={signed_headers}, Signature={signature}"
    )
    return headers


def _parse_xml(body: bytes) -> ET.Element:
    head = body[:1024].lstrip()
    if b"<!DOCTYPE" in head or b"<!ENTITY" in body:
        raise S3LiteError(
            "InvalidResponse", "XML response contains a DTD, refusing to parse"
        )
    return ET.fromstring(body)


def parse_list_response(body: bytes) -> Dict[str, Any]:
    root = _parse_xml(body)
    contents: List[Dict[str, Any]] = []
    for node in root.findall(f"{LIST_NAMESPACE}Contents"):
        key_node = node.find(f"{LIST_NAMESPACE}Key")
        if key_node is None or not key_node.text:
            continue
        size_node = node.find(f"{LIST_NAMESPACE}Size")
        contents.append(
            {
                "Key": key_node.text,
                "Size": int(size_node.text) if size_node is not None and size_node.text else 0,
            }
        )
    truncated_node = root.find(f"{LIST_NAMESPACE}IsTruncated")
    truncated = (truncated_node.text or "").strip().lower() == "true" if truncated_node is not None else False
    token_node = root.find(f"{LIST_NAMESPACE}NextContinuationToken")
    next_token = token_node.text if token_node is not None else None
    result: Dict[str, Any] = {"Contents": contents, "IsTruncated": truncated}
    if next_token:
        result["NextContinuationToken"] = next_token
    return result


def parse_error_response(body: bytes, status: int) -> S3LiteError:
    code = "404" if status == 404 else f"HTTP{status}"
    message = f"Request failed with status {status}"
    try:
        root = _parse_xml(body)
        code_node = root.find("Code")
        message_node = root.find("Message")
        if code_node is not None and code_node.text:
            code = code_node.text
        if message_node is not None and message_node.text:
            message = message_node.text
    except (ET.ParseError, S3LiteError):
        pass
    return S3LiteError(code, message, status)


class StreamingBody:
    def __init__(self, response: requests.Response):
        self._response = response
        self._raw = response.raw

    def read(self, amount: Optional[int] = None) -> bytes:
        if amount is None:
            return self._raw.read(decode_content=True)
        return self._raw.read(amount, decode_content=True)

    def close(self) -> None:
        self._response.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()


class _ListPaginator:
    def __init__(self, client: "S3LiteClient"):
        self._client = client

    def paginate(self, Bucket: str, Prefix: str = "", **_: Any) -> Iterator[Dict[str, Any]]:
        token: Optional[str] = None
        while True:
            page = self._client.list_objects_v2(
                Bucket=Bucket, Prefix=Prefix, ContinuationToken=token
            )
            yield page
            if not page.get("IsTruncated"):
                return
            token = page.get("NextContinuationToken")
            if not token:
                return


class S3LiteClient:
    def __init__(
        self,
        *,
        endpoint: str,
        access_key_id: str,
        secret_access_key: str,
        session_token: Optional[str] = None,
        region: str = "auto",
        max_pool_connections: int = 32,
        timeout: int = 60,
        max_attempts: int = 5,
    ):
        parsed = urlparse(endpoint)
        if not parsed.scheme or not parsed.netloc:
            raise ValueError(f"Invalid S3 endpoint: {endpoint!r}")

        self._scheme = parsed.scheme
        self._host = parsed.netloc
        self._access_key_id = access_key_id
        self._secret_access_key = secret_access_key
        self._session_token = session_token
        self._region = region or "auto"
        self._timeout = timeout
        self._max_attempts = max(1, max_attempts)

        self._session = requests.Session()
        adapter = HTTPAdapter(
            pool_connections=max_pool_connections,
            pool_maxsize=max_pool_connections,
            max_retries=0,
        )
        self._session.mount("https://", adapter)
        self._session.mount("http://", adapter)

    def get_paginator(self, name: str) -> _ListPaginator:
        if name != "list_objects_v2":
            raise ValueError(f"Unsupported paginator: {name}")
        return _ListPaginator(self)

    def _request(self, path: str, params: Dict[str, str], *, stream: bool) -> requests.Response:
        url = f"{self._scheme}://{self._host}{canonical_uri(path)}"
        last_error: Optional[Exception] = None
        for attempt in range(self._max_attempts):
            headers = build_authorization(
                method="GET",
                host=self._host,
                path=path,
                params=params,
                access_key=self._access_key_id,
                secret_key=self._secret_access_key,
                session_token=self._session_token,
                region=self._region,
                now=datetime.now(timezone.utc),
            )
            try:
                response = self._session.get(
                    url, params=params, headers=headers, timeout=self._timeout, stream=stream
                )
            except requests.RequestException as exc:
                last_error = exc
                if attempt + 1 >= self._max_attempts:
                    raise S3LiteError("ConnectionError", str(exc), None) from exc
                time.sleep(min(2**attempt * 0.1, 5.0))
                continue

            if response.status_code == 200:
                return response

            body = response.content
            response.close()
            if response.status_code in RETRY_STATUSES and attempt + 1 < self._max_attempts:
                time.sleep(min(2**attempt * 0.1, 5.0))
                continue
            raise parse_error_response(body, response.status_code)
        raise S3LiteError("RetriesExhausted", str(last_error), None)

    def list_objects_v2(
        self,
        Bucket: str,
        Prefix: str = "",
        ContinuationToken: Optional[str] = None,
        MaxKeys: Optional[int] = None,
        Delimiter: Optional[str] = None,
        **_: Any,
    ) -> Dict[str, Any]:
        params: Dict[str, str] = {"list-type": "2"}
        if Prefix:
            params["prefix"] = Prefix
        if ContinuationToken:
            params["continuation-token"] = ContinuationToken
        if MaxKeys:
            params["max-keys"] = str(MaxKeys)
        if Delimiter:
            params["delimiter"] = Delimiter
        response = self._request(f"/{Bucket}", params, stream=False)
        try:
            return parse_list_response(response.content)
        finally:
            response.close()

    def get_object(self, Bucket: str, Key: str, **_: Any) -> Dict[str, Any]:
        response = self._request(f"/{Bucket}/{Key}", {}, stream=True)
        return {
            "Body": StreamingBody(response),
            "ContentLength": int(response.headers.get("Content-Length", 0) or 0),
        }

    def close(self) -> None:
        self._session.close()
