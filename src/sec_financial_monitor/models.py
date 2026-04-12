from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SecFiling:
    cik: str
    ticker: str
    company_name: str
    accession_number: str
    filing_date: str
    report_date: str
    form_type: str
    primary_document: str
    description: str

    @property
    def filing_id(self) -> str:
        return self.accession_number

    @property
    def document_url(self) -> str:
        cik_int = str(int(self.cik))
        accession_nodash = self.accession_number.replace("-", "")
        return f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{accession_nodash}/{self.primary_document}"
