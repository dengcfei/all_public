from __future__ import annotations

REPORT_TYPE_ALIASES: dict[str, tuple[str, ...]] = {
    "annual": ("年度报告", "年报", "annual report"),
    "half": ("半年度报告", "中期报告", "interim report", "half-year"),
    "quarter": ("一季度报告", "三季度报告", "季度报告", "季报", "quarterly report"),
}


def detect_report_type(title: str) -> str | None:
    lowered = title.lower()

    for report_type, aliases in REPORT_TYPE_ALIASES.items():
        for alias in aliases:
            if alias.lower() in lowered:
                return report_type

    return None


def looks_like_financial_report(
    title: str,
    report_types: set[str],
    include_summary: bool,
) -> bool:
    report_type = detect_report_type(title)
    if report_type is None or report_type not in report_types:
        return False

    if include_summary:
        return True

    lowered = title.lower()
    if "摘要" in title or "summary" in lowered:
        return False

    return True
