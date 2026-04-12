from __future__ import annotations

from .models import SecFiling

SEC_FORM_TYPES: dict[str, tuple[str, ...]] = {
    "annual": ("10-K", "20-F", "40-F"),
    "quarter": ("10-Q",),
    "half": ("6-K",),
}

_HALF_YEAR_KEYWORDS = (
    "half-year",
    "half year",
    "semi-annual",
    "interim",
)


def resolve_form_types(report_types: set[str]) -> set[str]:
    forms: set[str] = set()
    for report_type in report_types:
        forms.update(SEC_FORM_TYPES.get(report_type, ()))
    return forms


def looks_like_financial_report(filing: SecFiling, report_types: set[str]) -> bool:
    form = filing.form_type.upper().strip()

    if "annual" in report_types and form in SEC_FORM_TYPES["annual"]:
        return True
    if "quarter" in report_types and form in SEC_FORM_TYPES["quarter"]:
        return True
    if "half" in report_types and form in SEC_FORM_TYPES["half"]:
        haystack = f"{filing.description} {filing.primary_document}".lower()
        return any(keyword in haystack for keyword in _HALF_YEAR_KEYWORDS)

    return False
