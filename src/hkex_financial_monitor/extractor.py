from __future__ import annotations

import re
from pathlib import Path

from pypdf import PdfReader

from .models import FinancialExtraction


_FIELD_ALIASES: dict[str, list[str]] = {
    "revenue": [
        "operating revenues",
        "revenue",
        "revenues",
        "turnover",
        "收入",
        "营业收入",
        "收益",
    ],
    "gross_profit": [
        "gross profit",
        "gross profits",
        "毛利",
        "毛利润",
        "毛利潤",
    ],
    "operating_profit": [
        "operating profit",
        "operating profits",
        "income from operations",
        "operating income",
        "营业利润",
        "經營盈利",
        "經營利潤",
        "经营盈利",
        "经营利润",
    ],
    "profit_for_period": [
        "profit for the period",
        "profit for the year",
        "net profit",
        "net profits",
        "net income",
        "期内利润",
        "期內盈利",
        "年内利润",
        "年內盈利",
        "净利润",
        "純利",
    ],
    "profit_attributable": [
        "profit attributable to equity holders of the company",
        "profit attributable to equity shareholders of the company",
        "profit attributable to owners of the company",
        "profit attributable to shareholders of the company",
        "profit attributable to owners of the parent",
        "net profits attributable to equity shareholders of the company",
        "net income attributable to ordinary shareholders",
        "net income attributable to shareholders",
        "income attributable to ordinary shareholders",
        "income attributable to shareholders",
        "股东应占利润",
        "股東應佔溢利",
        "股东应占溢利",
        "本公司权益持有人应占盈利",
        "本公司權益持有人應佔盈利",
        "母公司股东应占利润",
        "母公司股東應佔利潤",
    ],
    "basic_eps": [
        "eps (rmb per share) basic",
        "eps basic",
        "earnings per share attributable to equity shareholders of the company basic",
        "basic (rmb yuan)",
        "basic earnings per share",
        "每股基本盈利",
        "每股基本收益",
    ],
    "diluted_eps": [
        "eps (rmb per share) diluted",
        "eps diluted",
        "earnings per share attributable to equity shareholders of the company diluted",
        "diluted (rmb yuan)",
        "diluted earnings per share",
        "每股摊薄盈利",
        "每股攤薄盈利",
        "每股稀释收益",
        "每股稀釋收益",
    ],
}

_CURRENCY_PATTERNS: list[tuple[str, str, str]] = [
    (r"\((RMB|HK\$|US\$|USD|HKD)\s+in\s+(millions?|billions?)", "", ""),
    (r"\((人民币|港币|美元)(百万元|千元|亿元|万元)\)", "", ""),
    (r"(RMB|HK\$|US\$|USD|HKD)\s+in\s+(millions?|billions?)", "", ""),
    (r"(人民币|港币|美元)(百万元|千元|亿元|万元)", "", ""),
]


class PDFFinancialExtractor:
    def __init__(self, max_pages: int = 12) -> None:
        self.max_pages = max_pages

    def extract(self, pdf_path: Path) -> FinancialExtraction:
        reader = PdfReader(str(pdf_path))
        pages = []
        for page in reader.pages[: self.max_pages]:
            pages.append(page.extract_text() or "")

        raw_text = "\n".join(pages)
        flat_text = self._flatten_text(raw_text)
        currency, unit = self._detect_currency_and_unit(raw_text=raw_text, flat_text=flat_text)

        return FinancialExtraction(
            source_type="pdf",
            page_count=len(reader.pages),
            text_length=len(flat_text),
            currency=currency,
            unit=unit,
            revenue=self._extract_metric(flat_text, "revenue"),
            gross_profit=self._extract_metric(flat_text, "gross_profit"),
            operating_profit=self._extract_metric(flat_text, "operating_profit"),
            profit_for_period=self._extract_metric(flat_text, "profit_for_period"),
            profit_attributable=self._extract_metric(flat_text, "profit_attributable"),
            basic_eps=self._extract_metric(flat_text, "basic_eps"),
            diluted_eps=self._extract_metric(flat_text, "diluted_eps"),
        )

    @staticmethod
    def _flatten_text(text: str) -> str:
        return re.sub(r"\s+", " ", text).strip()

    def _extract_metric(self, flat_text: str, field_name: str) -> str:
        for alias in _FIELD_ALIASES[field_name]:
            value = self._match_value_after_label(flat_text, alias)
            if value:
                return value
        return ""

    def _match_value_after_label(self, flat_text: str, label: str) -> str:
        label_pattern = self._build_label_pattern(label)
        patterns = [
            rf"{label_pattern}\s*(?:[:：]|\b)?\s*(?:of\s+)?(?:RMB|HK\$|US\$|USD|HKD|人民币|港币|美元)?\s*({_number_pattern()})",
            rf"{label_pattern}\s+({_number_pattern()})",
        ]
        for pattern in patterns:
            match = re.search(pattern, flat_text, flags=re.IGNORECASE)
            if match:
                return match.group(1)
        return ""

    @staticmethod
    def _build_label_pattern(label: str) -> str:
        tokens = [re.escape(token) for token in label.split()]
        if len(tokens) == 1:
            return tokens[0]
        return r"\s+".join(tokens)

    @staticmethod
    def _detect_currency_and_unit(raw_text: str, flat_text: str) -> tuple[str, str]:
        for pattern, _, _ in _CURRENCY_PATTERNS:
            match = re.search(pattern, raw_text, flags=re.IGNORECASE)
            if not match:
                match = re.search(pattern, flat_text, flags=re.IGNORECASE)
            if not match:
                continue
            if len(match.groups()) >= 2:
                return _normalize_currency(match.group(1)), _normalize_unit(match.group(2))
            if len(match.groups()) == 1:
                return _normalize_currency(match.group(1)), ""

        currency_match = re.search(r"\b(RMB|HK\$|US\$|USD|HKD)\b", flat_text, flags=re.IGNORECASE)
        if currency_match:
            return _normalize_currency(currency_match.group(1)), ""
        cn_currency_match = re.search(r"(人民币|港币|美元)", flat_text)
        if cn_currency_match:
            return _normalize_currency(cn_currency_match.group(1)), ""
        return "", ""


def _number_pattern() -> str:
    return r"\(?-?\d[\d,]*(?:\.\d+)?\)?"


def _normalize_currency(value: str) -> str:
    lowered = value.strip().lower()
    if lowered in {"rmb", "人民币"}:
        return "RMB"
    if lowered in {"hk$", "hkd", "港币"}:
        return "HKD"
    if lowered in {"us$", "usd", "美元"}:
        return "USD"
    return value.strip().upper()


def _normalize_unit(value: str) -> str:
    lowered = value.strip().lower()
    mapping = {
        "million": "million",
        "millions": "million",
        "billion": "billion",
        "billions": "billion",
        "百万元": "million",
        "千元": "thousand",
        "亿元": "hundred_million",
        "万元": "ten_thousand",
    }
    return mapping.get(lowered, value.strip())
