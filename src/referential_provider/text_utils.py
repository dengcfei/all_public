from __future__ import annotations

from functools import lru_cache

from opencc import OpenCC


@lru_cache(maxsize=1)
def _converter_s2t() -> OpenCC:
    return OpenCC("s2t")


@lru_cache(maxsize=1)
def _converter_t2s() -> OpenCC:
    return OpenCC("t2s")


def _norm_text(value: str | None) -> str:
    return (value or "").strip()


def contains_cjk(value: str | None) -> bool:
    text = _norm_text(value)
    return any("\u4e00" <= char <= "\u9fff" for char in text)


def chinese_variants(value: str | None) -> list[str]:
    text = _norm_text(value)
    if not text:
        return []

    variants: list[str] = []
    for candidate in (text, _converter_s2t().convert(text), _converter_t2s().convert(text)):
        cleaned = _norm_text(candidate)
        if cleaned and cleaned not in variants:
            variants.append(cleaned)
    return variants
