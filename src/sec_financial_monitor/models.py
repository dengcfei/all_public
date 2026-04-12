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
    def accession_nodash(self) -> str:
        return self.accession_number.replace("-", "")

    @property
    def archive_dir_url(self) -> str:
        cik_int = str(int(self.cik))
        return f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{self.accession_nodash}"

    @property
    def txt_document_url(self) -> str:
        return f"{self.archive_dir_url}/{self.accession_nodash}.txt"

    @property
    def primary_document_url(self) -> str:
        return f"{self.archive_dir_url}/{self.primary_document}"

    @property
    def document_url(self) -> str:
        return self.primary_document_url
