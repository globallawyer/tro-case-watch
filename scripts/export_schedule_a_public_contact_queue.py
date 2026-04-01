#!/usr/bin/env python3

"""
Export a public-contact review queue from a Schedule A PDF.

This script intentionally limits itself to publicly accessible seller profile
links and manual-review fields. It does not scrape or harvest personal contact
details such as email addresses or phone numbers.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import zipfile
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable
from urllib.parse import quote_plus
from xml.sax.saxutils import escape

from Foundation import NSURL
from Quartz import PDFDocument


AMAZON_PLATFORM = "Amazon"
DEFAULT_MARKETPLACE_ID = "ATVPDKIKX0DER"
SELLER_PROFILE_TEMPLATE = "https://www.amazon.com/sp?marketplaceID={marketplace_id}&seller={seller_id}"
AMAZON_SEARCH_TEMPLATE = "https://www.amazon.com/s?k={query}"
BING_SEARCH_TEMPLATE = "https://www.bing.com/search?q={query}"
HEADER_MARKERS = {
    "Doe",
    "Seller Alias Platform Seller ID",
}


@dataclass
class SellerRecord:
    case_number: str
    source_pdf: str
    doe_no: int
    seller_alias: str
    platform: str
    seller_id: str
    amazon_seller_profile_url: str
    amazon_alias_search_url: str
    public_web_search_url: str
    contact_channel: str
    review_status: str
    notes: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract Schedule A sellers and create a public-contact review queue."
    )
    parser.add_argument("--pdf", required=True, help="Absolute path to the Schedule A PDF.")
    parser.add_argument(
        "--case-number",
        default="",
        help="Optional case number override. If omitted, the script tries to infer it from page text or file name.",
    )
    parser.add_argument(
        "--output-dir",
        default="data/contact-queue",
        help="Directory where CSV and JSON exports will be written. Defaults to data/contact-queue.",
    )
    parser.add_argument(
        "--marketplace-id",
        default=DEFAULT_MARKETPLACE_ID,
        help="Amazon marketplace ID to embed in seller profile links. Defaults to US marketplace.",
    )
    parser.add_argument(
        "--start-doe",
        type=int,
        default=1,
        help="Starting Doe number for the mobile launcher. Defaults to 1.",
    )
    parser.add_argument(
        "--message-template",
        default="",
        help="Optional default message template to preload into the mobile launcher.",
    )
    return parser.parse_args()


def load_pdf(path: Path) -> PDFDocument:
    url = NSURL.fileURLWithPath_(str(path))
    pdf = PDFDocument.alloc().initWithURL_(url)
    if pdf is None:
        raise RuntimeError(f"Unable to open PDF: {path}")
    return pdf


def extract_page_texts(pdf_path: Path) -> list[str]:
    pdf = load_pdf(pdf_path)
    texts: list[str] = []
    for page_index in range(pdf.pageCount()):
        page = pdf.pageAtIndex_(page_index)
        texts.append(str(page.string() or ""))
    return texts


def infer_case_number(page_texts: Iterable[str], pdf_path: Path, explicit_case_number: str) -> str:
    if explicit_case_number.strip():
        return explicit_case_number.strip()

    case_pattern = re.compile(r"Case\s+(\d+:\d+-cv-\d+(?:-[A-Za-z]+)?)", re.IGNORECASE)
    for text in page_texts:
        match = case_pattern.search(text)
        if match:
            return match.group(1)

    stem_match = re.search(r"(\d{2,4}-cv-\d{3,6})", pdf_path.stem, re.IGNORECASE)
    if stem_match:
        return stem_match.group(1)

    return pdf_path.stem


def clean_page_text(raw_text: str) -> str:
    cleaned_lines: list[str] = []

    for line in raw_text.splitlines():
        stripped = " ".join(line.split())
        if not stripped:
            continue
        if stripped in HEADER_MARKERS:
            continue
        if stripped.startswith("Case ") and "Page " in stripped:
            continue
        if stripped.startswith("No. "):
            continue
        if re.fullmatch(r"No\.\s*(\d+\s*)+", stripped):
            continue
        cleaned_lines.append(stripped)

    return "\n".join(cleaned_lines)


def extract_header_numbers(raw_text: str) -> list[int]:
    match = re.search(r"No\.\s+(.+?)\s+Case\s+\d+:\d+-cv-\d+", raw_text, re.IGNORECASE | re.DOTALL)
    if not match:
        return []
    return [int(value) for value in re.findall(r"\d+", match.group(1))]


def join_candidate_parts(parts: list[str]) -> str:
    if not parts:
        return ""

    merged = parts[0]
    for part in parts[1:]:
        previous_token = merged.split()[-1]
        next_token = part.split()[0]
        if (
            " " not in previous_token
            and previous_token.isalpha()
            and previous_token.islower()
            and next_token.isalpha()
            and next_token.islower()
            and " " not in merged
        ):
            merged += part
        else:
            merged += f" {part}"
    return merged


def parse_records(page_texts: Iterable[str], case_number: str, pdf_path: Path, marketplace_id: str) -> list[SellerRecord]:
    record_pattern = re.compile(r"^(?:(\d+)\s+)?(.*?)\s+Amazon\s+([A-Z0-9]+)$")
    records: list[SellerRecord] = []

    for page_text in page_texts:
        header_numbers = extract_header_numbers(page_text)
        cleaned_page = clean_page_text(page_text)
        pending_parts: list[str] = []
        sequential_index = 0
        for line in cleaned_page.splitlines():
            line = line.strip()
            if not line:
                continue

            if not pending_parts:
                if re.match(r"^\d+\s+", line):
                    pending_parts = [line]
                elif header_numbers:
                    pending_parts = [line]
                else:
                    continue
            else:
                pending_parts.append(line)

            candidate = join_candidate_parts(pending_parts)
            match = record_pattern.match(candidate)
            if not match:
                continue

            explicit_doe_no = match.group(1)
            if explicit_doe_no:
                doe_no = int(explicit_doe_no)
            else:
                if sequential_index >= len(header_numbers):
                    raise RuntimeError(f"Row count exceeded header number count for page text: {candidate}")
                doe_no = header_numbers[sequential_index]
            sequential_index += 1

            seller_alias = " ".join(match.group(2).split())
            seller_id = match.group(3).strip()
            amazon_profile_url = SELLER_PROFILE_TEMPLATE.format(
                marketplace_id=marketplace_id,
                seller_id=seller_id,
            )
            alias_query = quote_plus(f"site:amazon.com {seller_alias} {seller_id}")
            public_search_query = quote_plus(f'"{seller_alias}" "{seller_id}" Amazon')
            records.append(
                SellerRecord(
                    case_number=case_number,
                    source_pdf=str(pdf_path),
                    doe_no=doe_no,
                    seller_alias=seller_alias,
                    platform=AMAZON_PLATFORM,
                    seller_id=seller_id,
                    amazon_seller_profile_url=amazon_profile_url,
                    amazon_alias_search_url=AMAZON_SEARCH_TEMPLATE.format(query=alias_query),
                    public_web_search_url=BING_SEARCH_TEMPLATE.format(query=public_search_query),
                    contact_channel="Amazon seller profile / public business info / manual review",
                    review_status="needs_manual_review",
                    notes="Open the seller profile URL and use Amazon's public seller page or visible contact options if shown.",
                )
            )
            pending_parts = []

        if header_numbers and sequential_index != len(header_numbers):
            raise RuntimeError(
                f"Header number count mismatch: expected {len(header_numbers)} rows but parsed {sequential_index}"
            )

    deduped: dict[int, SellerRecord] = {}
    for record in records:
        deduped[record.doe_no] = record

    return [deduped[key] for key in sorted(deduped)]


def write_csv(records: list[SellerRecord], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(asdict(records[0]).keys()) if records else list(SellerRecord.__dataclass_fields__.keys())
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for record in records:
            writer.writerow(asdict(record))


def write_json(records: list[SellerRecord], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload = [asdict(record) for record in records]
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def column_name(index: int) -> str:
    result = ""
    current = index
    while current > 0:
        current, remainder = divmod(current - 1, 26)
        result = chr(65 + remainder) + result
    return result


def inline_string_cell(cell_ref: str, value: str, style_id: int | None = None) -> str:
    style_attr = f' s="{style_id}"' if style_id is not None else ""
    safe_value = escape(value)
    return f'<c r="{cell_ref}" t="inlineStr"{style_attr}><is><t>{safe_value}</t></is></c>'


def number_cell(cell_ref: str, value: int) -> str:
    return f'<c r="{cell_ref}"><v>{value}</v></c>'


def build_sheet_xml(records: list[SellerRecord]) -> tuple[str, str]:
    headers = [
        "Case Number",
        "Doe No",
        "Seller Alias",
        "Seller ID",
        "Amazon Contact Page",
        "Amazon Search",
        "Public Web Search",
        "Contact Notes",
    ]
    rows_xml: list[str] = []
    hyperlink_entries: list[tuple[str, str, str]] = []

    header_cells = []
    for index, header in enumerate(headers, start=1):
        cell_ref = f"{column_name(index)}1"
        header_cells.append(inline_string_cell(cell_ref, header, style_id=1))
    rows_xml.append(f'<row r="1">{"".join(header_cells)}</row>')

    for row_number, record in enumerate(records, start=2):
        row_cells = [
            inline_string_cell(f"A{row_number}", record.case_number),
            number_cell(f"B{row_number}", record.doe_no),
            inline_string_cell(f"C{row_number}", record.seller_alias),
            inline_string_cell(f"D{row_number}", record.seller_id),
            inline_string_cell(f"E{row_number}", "打开Amazon店铺页", style_id=2),
            inline_string_cell(f"F{row_number}", "打开Amazon搜索", style_id=2),
            inline_string_cell(f"G{row_number}", "打开公开搜索", style_id=2),
            inline_string_cell(f"H{row_number}", record.notes),
        ]
        rows_xml.append(f'<row r="{row_number}">{"".join(row_cells)}</row>')
        hyperlink_entries.append((f"E{row_number}", f"rId{len(hyperlink_entries) + 1}", record.amazon_seller_profile_url))
        hyperlink_entries.append((f"F{row_number}", f"rId{len(hyperlink_entries) + 1}", record.amazon_alias_search_url))
        hyperlink_entries.append((f"G{row_number}", f"rId{len(hyperlink_entries) + 1}", record.public_web_search_url))

    hyperlinks_xml = "".join(
        f'<hyperlink ref="{cell_ref}" r:id="{rel_id}"/>'
        for cell_ref, rel_id, _target in hyperlink_entries
    )
    sheet_xml = f'''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"
 xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
  <sheetViews>
    <sheetView workbookViewId="0">
      <pane ySplit="1" topLeftCell="A2" activePane="bottomLeft" state="frozen"/>
    </sheetView>
  </sheetViews>
  <sheetFormatPr defaultRowHeight="15"/>
  <cols>
    <col min="1" max="1" width="18" customWidth="1"/>
    <col min="2" max="2" width="10" customWidth="1"/>
    <col min="3" max="3" width="38" customWidth="1"/>
    <col min="4" max="4" width="20" customWidth="1"/>
    <col min="5" max="7" width="24" customWidth="1"/>
    <col min="8" max="8" width="66" customWidth="1"/>
  </cols>
  <sheetData>
    {''.join(rows_xml)}
  </sheetData>
  <hyperlinks>{hyperlinks_xml}</hyperlinks>
</worksheet>
'''

    rels_xml = ['<?xml version="1.0" encoding="UTF-8" standalone="yes"?>']
    rels_xml.append('<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">')
    for cell_ref, rel_id, target in hyperlink_entries:
        del cell_ref
        rels_xml.append(
            f'<Relationship Id="{rel_id}" '
            'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/hyperlink" '
            f'Target="{escape(target)}" TargetMode="External"/>'
        )
    rels_xml.append("</Relationships>")
    return sheet_xml, "".join(rels_xml)


def write_xlsx(records: list[SellerRecord], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    sheet_xml, sheet_rels_xml = build_sheet_xml(records)
    workbook_xml = '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"
 xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
  <sheets>
    <sheet name="Public Contact Queue" sheetId="1" r:id="rId1"/>
  </sheets>
</workbook>
'''
    workbook_rels_xml = '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>
  <Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>
</Relationships>
'''
    root_rels_xml = '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>
  <Relationship Id="rId2" Type="http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties" Target="docProps/core.xml"/>
  <Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/extended-properties" Target="docProps/app.xml"/>
</Relationships>
'''
    content_types_xml = '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml" ContentType="application/xml"/>
  <Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
  <Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
  <Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>
  <Override PartName="/docProps/core.xml" ContentType="application/vnd.openxmlformats-package.core-properties+xml"/>
  <Override PartName="/docProps/app.xml" ContentType="application/vnd.openxmlformats-officedocument.extended-properties+xml"/>
</Types>
'''
    styles_xml = '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
  <fonts count="2">
    <font>
      <sz val="11"/>
      <name val="Aptos"/>
      <family val="2"/>
    </font>
    <font>
      <sz val="11"/>
      <name val="Aptos"/>
      <family val="2"/>
      <color rgb="FF0563C1"/>
      <u/>
    </font>
  </fonts>
  <fills count="2">
    <fill><patternFill patternType="none"/></fill>
    <fill><patternFill patternType="gray125"/></fill>
  </fills>
  <borders count="1">
    <border><left/><right/><top/><bottom/><diagonal/></border>
  </borders>
  <cellStyleXfs count="1">
    <xf numFmtId="0" fontId="0" fillId="0" borderId="0"/>
  </cellStyleXfs>
  <cellXfs count="3">
    <xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/>
    <xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0">
      <alignment horizontal="center"/>
    </xf>
    <xf numFmtId="0" fontId="1" fillId="0" borderId="0" xfId="0"/>
  </cellXfs>
  <cellStyles count="1">
    <cellStyle name="Normal" xfId="0" builtinId="0"/>
  </cellStyles>
</styleSheet>
'''
    core_xml = '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<cp:coreProperties xmlns:cp="http://schemas.openxmlformats.org/package/2006/metadata/core-properties"
 xmlns:dc="http://purl.org/dc/elements/1.1/"
 xmlns:dcterms="http://purl.org/dc/terms/"
 xmlns:dcmitype="http://purl.org/dc/dcmitype/"
 xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <dc:creator>Codex</dc:creator>
  <cp:lastModifiedBy>Codex</cp:lastModifiedBy>
</cp:coreProperties>
'''
    app_xml = '''<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Properties xmlns="http://schemas.openxmlformats.org/officeDocument/2006/extended-properties"
 xmlns:vt="http://schemas.openxmlformats.org/officeDocument/2006/docPropsVTypes">
  <Application>Microsoft Excel</Application>
</Properties>
'''

    with zipfile.ZipFile(output_path, "w", compression=zipfile.ZIP_DEFLATED) as workbook:
        workbook.writestr("[Content_Types].xml", content_types_xml)
        workbook.writestr("_rels/.rels", root_rels_xml)
        workbook.writestr("docProps/core.xml", core_xml)
        workbook.writestr("docProps/app.xml", app_xml)
        workbook.writestr("xl/workbook.xml", workbook_xml)
        workbook.writestr("xl/_rels/workbook.xml.rels", workbook_rels_xml)
        workbook.writestr("xl/styles.xml", styles_xml)
        workbook.writestr("xl/worksheets/sheet1.xml", sheet_xml)
        workbook.writestr("xl/worksheets/_rels/sheet1.xml.rels", sheet_rels_xml)


def build_mobile_html(records: list[SellerRecord], case_number: str, start_doe: int = 1, message_template: str = "") -> str:
    payload = [
        {
            "doe_no": record.doe_no,
            "seller_alias": record.seller_alias,
            "seller_id": record.seller_id,
            "amazon_seller_profile_url": record.amazon_seller_profile_url,
            "amazon_alias_search_url": record.amazon_alias_search_url,
            "public_web_search_url": record.public_web_search_url,
        }
        for record in records
    ]
    payload_json = json.dumps(payload, ensure_ascii=False).replace("</", "<\\/")
    title = f"{case_number} Amazon Contact Launcher"
    default_start_index = 0
    for index, record in enumerate(records):
        if record.doe_no == start_doe:
            default_start_index = index
            break
    return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
  <title>{escape(title)}</title>
  <style>
    :root {{
      --bg: #f5f1e8;
      --card: #fffdf8;
      --ink: #1f2328;
      --muted: #586069;
      --line: #e5dccb;
      --accent: #b42318;
      --accent-dark: #7a1b12;
      --accent-soft: #fff1ef;
      --ok: #166534;
      --ok-soft: #eefbf2;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      background:
        radial-gradient(circle at top left, rgba(180,35,24,0.08), transparent 28%),
        linear-gradient(180deg, #f8f4ec 0%, #efe6d8 100%);
      color: var(--ink);
      font-family: -apple-system, BlinkMacSystemFont, "SF Pro Text", "PingFang SC", sans-serif;
    }}
    .app {{
      max-width: 760px;
      margin: 0 auto;
      padding: 18px 14px 40px;
    }}
    .hero {{
      background: linear-gradient(135deg, #7a1b12 0%, #b42318 55%, #cf4a3a 100%);
      color: white;
      border-radius: 24px;
      padding: 18px;
      box-shadow: 0 16px 40px rgba(122, 27, 18, 0.22);
    }}
    .hero h1 {{
      margin: 0 0 8px;
      font-size: 24px;
      line-height: 1.15;
    }}
    .hero p {{
      margin: 0;
      font-size: 14px;
      line-height: 1.5;
      color: rgba(255,255,255,0.88);
    }}
    .panel {{
      margin-top: 14px;
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: 22px;
      padding: 16px;
      box-shadow: 0 8px 24px rgba(74, 55, 30, 0.08);
    }}
    .panel h2 {{
      margin: 0 0 10px;
      font-size: 17px;
    }}
    .stats {{
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 10px;
    }}
    .stat {{
      background: #faf4ea;
      border-radius: 16px;
      padding: 12px;
      border: 1px solid #eee1c9;
    }}
    .stat strong {{
      display: block;
      font-size: 22px;
      margin-bottom: 4px;
    }}
    .stat span {{
      color: var(--muted);
      font-size: 12px;
    }}
    .progress-wrap {{
      margin-top: 12px;
      background: #f1e7d7;
      border-radius: 999px;
      height: 12px;
      overflow: hidden;
    }}
    .progress-bar {{
      height: 100%;
      width: 0%;
      background: linear-gradient(90deg, #7a1b12, #cf4a3a);
      transition: width 180ms ease;
    }}
    label {{
      display: block;
      font-size: 13px;
      color: var(--muted);
      margin-bottom: 8px;
    }}
    textarea, input {{
      width: 100%;
      border-radius: 16px;
      border: 1px solid var(--line);
      padding: 12px 14px;
      font: inherit;
      background: #fffdfa;
      color: var(--ink);
    }}
    textarea {{
      min-height: 136px;
      resize: vertical;
      line-height: 1.5;
    }}
    .toolbar, .nav {{
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      margin-top: 12px;
    }}
    button, .btn {{
      appearance: none;
      border: 0;
      text-decoration: none;
      border-radius: 16px;
      padding: 13px 14px;
      font: inherit;
      font-weight: 700;
      text-align: center;
      cursor: pointer;
      min-height: 48px;
    }}
    .btn-primary {{
      background: var(--accent);
      color: white;
      flex: 1 1 180px;
    }}
    .btn-secondary {{
      background: #f4ebe0;
      color: var(--ink);
      flex: 1 1 140px;
    }}
    .btn-success {{
      background: var(--ok-soft);
      color: var(--ok);
      border: 1px solid rgba(22,101,52,0.16);
      flex: 1 1 180px;
    }}
    .card {{
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: 22px;
      padding: 16px;
      margin-top: 14px;
      box-shadow: 0 8px 24px rgba(74, 55, 30, 0.08);
    }}
    .badge {{
      display: inline-flex;
      align-items: center;
      gap: 6px;
      padding: 6px 10px;
      border-radius: 999px;
      background: var(--accent-soft);
      color: var(--accent-dark);
      font-size: 12px;
      font-weight: 700;
    }}
    .badge.done {{
      background: var(--ok-soft);
      color: var(--ok);
    }}
    .seller-title {{
      margin: 12px 0 6px;
      font-size: 25px;
      line-height: 1.15;
    }}
    .seller-meta {{
      color: var(--muted);
      font-size: 14px;
      line-height: 1.6;
    }}
    .hint {{
      margin-top: 10px;
      padding: 12px;
      border-radius: 16px;
      background: #faf4ea;
      color: #6d4d25;
      font-size: 13px;
      line-height: 1.5;
    }}
    .small {{
      color: var(--muted);
      font-size: 12px;
      line-height: 1.5;
      margin-top: 10px;
    }}
    .pill-row {{
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      margin-top: 12px;
    }}
    .pill {{
      border-radius: 999px;
      background: #f4ebe0;
      color: var(--ink);
      padding: 8px 10px;
      font-size: 12px;
      font-weight: 600;
    }}
    .steps {{
      margin-top: 12px;
      display: grid;
      gap: 8px;
    }}
    .step {{
      background: #fff8ef;
      border: 1px solid #f0e1ca;
      border-radius: 14px;
      padding: 10px 12px;
      font-size: 13px;
      color: #6d4d25;
      line-height: 1.45;
    }}
    @media (max-width: 520px) {{
      .stats {{
        grid-template-columns: 1fr;
      }}
      .seller-title {{
        font-size: 22px;
      }}
    }}
  </style>
</head>
<body>
  <div class="app">
    <section class="hero">
      <h1>{escape(case_number)} 联系启动台</h1>
      <p>适合 iPhone 使用。逐个打开 Amazon 店铺页，复制统一消息，手工完成站内最终发送。</p>
    </section>

    <section class="panel">
      <h2>消息模板</h2>
      <label for="messageTemplate">先把你的统一消息粘贴到这里，后面点一次“复制模板并打开当前店铺”即可。</label>
      <textarea id="messageTemplate" placeholder="请粘贴你的统一消息模板。"></textarea>
      <div class="toolbar">
        <button class="btn-primary" id="copyTemplateBtn">复制消息模板</button>
        <button class="btn-secondary" id="copySellerInfoBtn">复制当前卖家信息</button>
      </div>
      <div class="small">这不是自动群发器。它会帮你减少重复操作，但最后的 Amazon 页面点击和发送由你自己完成。</div>
    </section>

    <section class="panel">
      <h2>进度</h2>
      <div class="stats">
        <div class="stat"><strong id="doneCount">0</strong><span>已联系</span></div>
        <div class="stat"><strong id="remainingCount">0</strong><span>未完成</span></div>
        <div class="stat"><strong id="currentPosition">1 / 1</strong><span>当前序号</span></div>
      </div>
      <div class="progress-wrap"><div class="progress-bar" id="progressBar"></div></div>
      <div class="toolbar">
        <button class="btn-secondary" id="prevBtn">上一个</button>
        <button class="btn-secondary" id="nextPendingBtn">下一个未联系</button>
        <button class="btn-secondary" id="nextBtn">下一个</button>
      </div>
      <div class="toolbar">
        <input id="jumpInput" placeholder="输入 Doe / 店铺名 / Seller ID">
        <button class="btn-secondary" id="jumpBtn">查找</button>
      </div>
      <div class="toolbar">
        <button class="btn-secondary" id="resetBtn">重置到 Doe #{start_doe}</button>
      </div>
    </section>

    <section class="card">
      <span class="badge" id="statusBadge">未联系</span>
      <h2 class="seller-title" id="sellerAlias">Seller</h2>
      <div class="seller-meta" id="sellerMeta"></div>
      <div class="pill-row">
        <div class="pill" id="sellerIdPill">Seller ID</div>
        <div class="pill" id="doePill">Doe</div>
      </div>
      <div class="hint">推荐顺序已经压缩成两步：先点“复制模板并打开当前店铺”，完成 Amazon 页面里的手工发送；回到这里后，再点“标记已联系并打开下一个未联系”。</div>
      <div class="steps">
        <div class="step">1. 打开卖家页后，点截图里的 <strong>Ask a question</strong>。</div>
        <div class="step">2. 继续手工点 <strong>item</strong> 和 <strong>other</strong>。</div>
        <div class="step">3. 粘贴刚才自动复制好的模板，发送后返回本页。</div>
      </div>
      <div class="toolbar">
        <button class="btn-primary" id="launchCurrentBtn">复制模板并打开当前店铺</button>
        <a class="btn btn-primary" id="openSellerBtn" target="_blank" rel="noopener noreferrer">打开Amazon店铺页</a>
      </div>
      <div class="toolbar">
        <button class="btn-success" id="markDoneLaunchNextBtn">标记已联系并打开下一个未联系</button>
        <a class="btn btn-secondary" id="openPublicSearchBtn" target="_blank" rel="noopener noreferrer">打开公开搜索</a>
        <a class="btn btn-secondary" id="openAmazonSearchBtn" target="_blank" rel="noopener noreferrer">打开Amazon搜索</a>
      </div>
      <div class="toolbar">
        <button class="btn-secondary" id="markDoneBtn">仅标记已联系</button>
        <button class="btn-secondary" id="toggleDoneBtn">切换已联系状态</button>
        <button class="btn-secondary" id="copySellerLinkBtn">复制店铺页链接</button>
      </div>
      <div class="small">进度保存在当前浏览器本地。换手机或清理 Safari 数据后，记录不会自动同步。</div>
    </section>
  </div>

  <script>
    const CASE_NUMBER = {json.dumps(case_number, ensure_ascii=False)};
    const DEFAULT_START_DOE = {start_doe};
    const DEFAULT_START_INDEX = {default_start_index};
    const DEFAULT_TEMPLATE = {json.dumps(message_template, ensure_ascii=False)};
    const RECORDS = {payload_json};
    const STORAGE_KEY = `contact-launcher:${{CASE_NUMBER}}:start${{DEFAULT_START_DOE}}`;

    const refs = {{
      messageTemplate: document.getElementById("messageTemplate"),
      copyTemplateBtn: document.getElementById("copyTemplateBtn"),
      copySellerInfoBtn: document.getElementById("copySellerInfoBtn"),
      doneCount: document.getElementById("doneCount"),
      remainingCount: document.getElementById("remainingCount"),
      currentPosition: document.getElementById("currentPosition"),
      progressBar: document.getElementById("progressBar"),
      prevBtn: document.getElementById("prevBtn"),
      nextPendingBtn: document.getElementById("nextPendingBtn"),
      nextBtn: document.getElementById("nextBtn"),
      jumpInput: document.getElementById("jumpInput"),
      jumpBtn: document.getElementById("jumpBtn"),
      resetBtn: document.getElementById("resetBtn"),
      statusBadge: document.getElementById("statusBadge"),
      sellerAlias: document.getElementById("sellerAlias"),
      sellerMeta: document.getElementById("sellerMeta"),
      sellerIdPill: document.getElementById("sellerIdPill"),
      doePill: document.getElementById("doePill"),
      launchCurrentBtn: document.getElementById("launchCurrentBtn"),
      openSellerBtn: document.getElementById("openSellerBtn"),
      openAmazonSearchBtn: document.getElementById("openAmazonSearchBtn"),
      openPublicSearchBtn: document.getElementById("openPublicSearchBtn"),
      markDoneBtn: document.getElementById("markDoneBtn"),
      markDoneLaunchNextBtn: document.getElementById("markDoneLaunchNextBtn"),
      toggleDoneBtn: document.getElementById("toggleDoneBtn"),
      copySellerLinkBtn: document.getElementById("copySellerLinkBtn"),
    }};

    function loadState() {{
      try {{
        const parsed = JSON.parse(localStorage.getItem(STORAGE_KEY) || "{{}}");
        return {{
          index: Number.isInteger(parsed.index) ? parsed.index : DEFAULT_START_INDEX,
          done: Array.isArray(parsed.done) ? parsed.done : [],
          template: typeof parsed.template === "string" ? parsed.template : DEFAULT_TEMPLATE,
        }};
      }} catch {{
        return {{ index: DEFAULT_START_INDEX, done: [], template: DEFAULT_TEMPLATE }};
      }}
    }}

    const state = loadState();
    const doneSet = new Set(state.done);
    refs.messageTemplate.value = state.template;

    function saveState() {{
      localStorage.setItem(STORAGE_KEY, JSON.stringify({{
        index: state.index,
        done: Array.from(doneSet).sort((a, b) => a - b),
        template: refs.messageTemplate.value,
      }}));
    }}

    async function copyText(text, okMessage = "", noisy = true) {{
      if (!text.trim()) {{
        if (noisy) {{
          alert("内容为空，请先填写。");
        }}
        return false;
      }}
      try {{
        await navigator.clipboard.writeText(text);
        if (okMessage) {{
          alert(okMessage);
        }}
        return true;
      }} catch {{
        if (noisy) {{
          refs.messageTemplate.focus();
          refs.messageTemplate.select();
          alert("系统未授予剪贴板权限，请手工复制。");
        }}
        return false;
      }}
    }}

    function currentRecord() {{
      return RECORDS[state.index] || RECORDS[0];
    }}

    function findNextPendingIndex(startIndex = state.index) {{
      for (let index = startIndex + 1; index < RECORDS.length; index += 1) {{
        if (!doneSet.has(RECORDS[index].doe_no)) {{
          return index;
        }}
      }}
      return -1;
    }}

    function render() {{
      const record = currentRecord();
      const doneCount = doneSet.size;
      const remaining = RECORDS.length - doneCount;
      refs.doneCount.textContent = String(doneCount);
      refs.remainingCount.textContent = String(remaining);
      refs.currentPosition.textContent = `Doe #${{record.doe_no}} · ${{state.index + 1}} / ${{RECORDS.length}}`;
      refs.progressBar.style.width = `${{(doneCount / RECORDS.length) * 100}}%`;
      refs.sellerAlias.textContent = record.seller_alias;
      refs.sellerMeta.innerHTML = `Doe #${{record.doe_no}}<br>Seller ID: ${{record.seller_id}}`;
      refs.sellerIdPill.textContent = `Seller ID: ${{record.seller_id}}`;
      refs.doePill.textContent = `Doe #${{record.doe_no}}`;
      refs.openSellerBtn.href = record.amazon_seller_profile_url;
      refs.openAmazonSearchBtn.href = record.amazon_alias_search_url;
      refs.openPublicSearchBtn.href = record.public_web_search_url;
      const isDone = doneSet.has(record.doe_no);
      refs.statusBadge.textContent = isDone ? "已联系" : "未联系";
      refs.statusBadge.className = isDone ? "badge done" : "badge";
      refs.prevBtn.disabled = state.index <= 0;
      refs.nextBtn.disabled = state.index >= RECORDS.length - 1;
      refs.nextPendingBtn.disabled = findNextPendingIndex() === -1;
      saveState();
    }}

    function jumpToIndex(nextIndex) {{
      if (nextIndex < 0 || nextIndex >= RECORDS.length) {{
        return;
      }}
      state.index = nextIndex;
      render();
      window.scrollTo({{ top: 0, behavior: "smooth" }});
    }}

    function jumpToQuery(rawQuery) {{
      const query = String(rawQuery || "").trim().toLowerCase();
      if (!query) {{
        alert("请输入 Doe / 店铺名 / Seller ID。");
        return;
      }}

      let index = -1;
      if (/^\d+$/.test(query)) {{
        const doeNo = Number.parseInt(query, 10);
        index = RECORDS.findIndex((item) => item.doe_no === doeNo);
      }} else {{
        index = RECORDS.findIndex(
          (item) =>
            item.seller_alias.toLowerCase().includes(query) ||
            item.seller_id.toLowerCase().includes(query)
        );
      }}

      if (index === -1) {{
        alert("没有找到匹配的卖家。");
        return;
      }}
      jumpToIndex(index);
    }}

    function markCurrentDone(andNext) {{
      const record = currentRecord();
      doneSet.add(record.doe_no);
      if (andNext && state.index < RECORDS.length - 1) {{
        state.index += 1;
      }}
      render();
    }}

    async function launchCurrentSeller() {{
      const template = refs.messageTemplate.value.trim();
      if (template) {{
        await copyText(template, "", false);
      }}
      window.location.href = currentRecord().amazon_seller_profile_url;
    }}

    async function markDoneAndLaunchNextPending() {{
      const record = currentRecord();
      doneSet.add(record.doe_no);
      const nextPendingIndex = findNextPendingIndex(state.index);
      if (nextPendingIndex === -1) {{
        render();
        alert("已经没有未联系的卖家了。");
        return;
      }}
      state.index = nextPendingIndex;
      render();
      await launchCurrentSeller();
    }}

    refs.messageTemplate.addEventListener("input", saveState);
    refs.copyTemplateBtn.addEventListener("click", () => copyText(refs.messageTemplate.value, "消息模板已复制。"));
    refs.copySellerInfoBtn.addEventListener("click", () => {{
      const record = currentRecord();
      const text = `Case: ${{CASE_NUMBER}}\\nDoe: ${{record.doe_no}}\\nSeller: ${{record.seller_alias}}\\nSeller ID: ${{record.seller_id}}\\nLink: ${{record.amazon_seller_profile_url}}`;
      copyText(text, "当前卖家信息已复制。");
    }});
    refs.copySellerLinkBtn.addEventListener("click", () => copyText(currentRecord().amazon_seller_profile_url, "店铺页链接已复制。"));
    refs.launchCurrentBtn.addEventListener("click", launchCurrentSeller);
    refs.prevBtn.addEventListener("click", () => jumpToIndex(state.index - 1));
    refs.nextPendingBtn.addEventListener("click", () => {{
      const index = findNextPendingIndex();
      if (index === -1) {{
        alert("后面没有未联系的卖家了。");
        return;
      }}
      jumpToIndex(index);
    }});
    refs.nextBtn.addEventListener("click", () => jumpToIndex(state.index + 1));
    refs.jumpBtn.addEventListener("click", () => jumpToQuery(refs.jumpInput.value));
    refs.jumpInput.addEventListener("keydown", (event) => {{
      if (event.key === "Enter") {{
        jumpToQuery(refs.jumpInput.value);
      }}
    }});
    refs.resetBtn.addEventListener("click", () => {{
      state.index = DEFAULT_START_INDEX;
      doneSet.clear();
      refs.messageTemplate.value = DEFAULT_TEMPLATE;
      render();
      alert(`已重置到 Doe #${{DEFAULT_START_DOE}}。`);
    }});
    refs.markDoneBtn.addEventListener("click", () => markCurrentDone(false));
    refs.markDoneLaunchNextBtn.addEventListener("click", markDoneAndLaunchNextPending);
    refs.toggleDoneBtn.addEventListener("click", () => {{
      const record = currentRecord();
      if (doneSet.has(record.doe_no)) {{
        doneSet.delete(record.doe_no);
      }} else {{
        doneSet.add(record.doe_no);
      }}
      render();
    }});

    render();
  </script>
</body>
</html>
"""


def write_mobile_html(
    records: list[SellerRecord],
    case_number: str,
    output_path: Path,
    start_doe: int = 1,
    message_template: str = "",
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        build_mobile_html(records, case_number, start_doe=start_doe, message_template=message_template),
        encoding="utf-8",
    )


def build_output_base(case_number: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9._-]+", "-", case_number).strip("-")
    return slug or "schedule-a-contact-queue"


def main() -> int:
    args = parse_args()
    pdf_path = Path(args.pdf).expanduser().resolve()
    if not pdf_path.exists():
        print(f"PDF not found: {pdf_path}", file=sys.stderr)
        return 1

    page_texts = extract_page_texts(pdf_path)
    case_number = infer_case_number(page_texts, pdf_path, args.case_number)
    records = parse_records(page_texts, case_number, pdf_path, args.marketplace_id)

    if not records:
        print("No seller rows were parsed from the PDF.", file=sys.stderr)
        return 2

    output_dir = Path(args.output_dir)
    if not output_dir.is_absolute():
        output_dir = (Path(__file__).resolve().parents[1] / output_dir).resolve()
    output_base = build_output_base(case_number)
    csv_path = output_dir / f"{output_base}-public-contact-queue.csv"
    json_path = output_dir / f"{output_base}-public-contact-queue.json"
    xlsx_path = output_dir / f"{output_base}-public-contact-queue.xlsx"
    html_path = output_dir / f"{output_base}-contact-launcher-mobile.html"

    write_csv(records, csv_path)
    write_json(records, json_path)
    write_xlsx(records, xlsx_path)
    write_mobile_html(
        records,
        case_number,
        html_path,
        start_doe=args.start_doe,
        message_template=args.message_template,
    )

    print(f"Parsed {len(records)} seller records")
    print(f"Case number: {case_number}")
    print(f"CSV: {csv_path}")
    print(f"JSON: {json_path}")
    print(f"XLSX: {xlsx_path}")
    print(f"HTML: {html_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
