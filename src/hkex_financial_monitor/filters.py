from __future__ import annotations

import re

from .models import Announcement


# Broad keywords to capture annual/interim/results/financial/ESG style filings.
FINANCIAL_PATTERNS = [
    r"年報",
    r"中期報告",
    r"季度報告",
    r"財務報表",
    r"業績",
    r"盈利",
    r"環境.?社會.?管治|ESG",
    r"annual\s+report",
    r"interim\s+report",
    r"quarterly\s+report",
    r"financial\s+statements?",
    r"results?",
]

_FINANCIAL_RE = re.compile("|".join(FINANCIAL_PATTERNS), re.IGNORECASE)
HKEX_FINANCIAL_T1_CODE = "40000"


def looks_like_financial_report(item: Announcement) -> bool:
    # HKEX category code for "Fin. Stats. / ESG Info."
    if item.t1_code == HKEX_FINANCIAL_T1_CODE:
        return True

    haystack = " ".join(
        [
            item.title,
            item.category_long,
            item.category_short,
        ]
    )
    return bool(_FINANCIAL_RE.search(haystack))
