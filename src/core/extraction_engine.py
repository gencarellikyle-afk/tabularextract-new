# src/core/extraction_engine.py
import os
import tempfile
import json
import re
from io import BytesIO
from typing import Dict, List, Any
import pandas as pd
import camelot
import pdfplumber
from anthropic import Anthropic

class TableExtractionEngine:
    def __init__(self):
        self.client = Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
        
        # === YOUR EXACT PROMPTS (unchanged) ===
        self.PERFECTION_PROMPT = """You are the world's #1 PDF table extraction expert. Turn this raw table into perfect Excel-ready CSV + JSON.
STRICT RULES (NEVER break these):
- Use ONLY the exact printed headers from the document. NEVER output Column_0, Column header, etc.
- Repeat section names in every row for hierarchy.
- For merged cells: put full text in LEFTMOST column only.
- Convert symbols: ☒→No, ✓→Yes.
- Keep commas in numbers.
- Output ONLY this JSON: {"csv": "...", "json": [...], "confidence": 0.99}"""

        self.RESCUE_PROMPT = """You have the FULL PAGE TEXT + raw table. Reconstruct using ONLY exact printed headers visible on the page. Fix duplicates, shifts, merged cells perfectly. Never use Column_0 or generic names. Output ONLY this JSON: {"csv": "...", "json": [...], "confidence": 0.99}"""

    # === YOUR EXISTING HELPERS (unchanged) ===
    def handle_merged_cells(self, df: pd.DataFrame) -> pd.DataFrame:
        if len(df.columns) < 2 or len(df) == 0:
            return df
        try:
            for col_idx in range(1, len(df.columns)):
                prev = df.iloc[:, col_idx-1].astype(str).str.strip()
                curr = df.iloc[:, col_idx].astype(str).str.strip()
                for row in range(len(df)):
                    if prev.iloc[row] == curr.iloc[row] and prev.iloc[row] != "":
                        df.iloc[row, col_idx] = ""
        except:
            pass
        return df

    def extract_json_safe(self, text: str) -> Dict:
        text = re.sub(r'[\x00-\x1F\x7F]', '', text)
        match = re.search(r'\{.*\}', text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group(0))
            except:
                pass
        return {"csv": "", "json": [], "confidence": 0.0}

    def final_polish(self, df: pd.DataFrame) -> pd.DataFrame:
        """Protects real printed headers like “Expenditure by function £ million” and “Role”."""
        if df.empty:
            return df
        new_cols = []
        for i, col in enumerate(df.columns):
            col_str = str(col).strip()
            cleaned = re.sub(r'Column header \(TH\)|Row header \(TH\)|Data cell \(TD\)|\(TH\)|\(TD\)|Unnamed: \d+|Column_\d+|Column \d+', '', col_str, flags=re.IGNORECASE)
            cleaned = cleaned.strip()
            new_cols.append(cleaned if cleaned else f"Column_{i}")
        df.columns = new_cols
        df = df.replace(['', 'nan', 'NaN', 'None'], '').fillna('')
        return df

    def _get_full_page_context(self, pdf_path: str, page_numbers: List[int]) -> str:
        context = []
        try:
            with pdfplumber.open(pdf_path) as pdf:
                for p_num in page_numbers:
                    if 0 <= p_num - 1 < len(pdf.pages):
                        context.append(pdf.pages[p_num - 1].extract_text() or "")
        except:
            pass
        return "\n\n".join(context).strip()

    def _needs_rescue(self, df: pd.DataFrame, confidence: float) -> bool:
        """Fully general — works on ANY PDF. No benchmark-specific keywords."""
        if confidence < 0.94:
            return True
        if df.empty or len(df.columns) == 0:
            return True

        cols = [str(c).strip().lower() for c in df.columns]

        # Generic/bad headers
        if any(c.startswith('column_') or c in ['', 'unnamed', 'none'] for c in cols):
            return True

        # Header-shift detection (first row looks like data)
        if len(cols) > 0 and (cols[0].replace(',', '').replace('.', '').isdigit() or any(x.isdigit() for x in cols[0].split())):
            return True

        # Too many empty/weak headers
        empty_ratio = sum(1 for c in cols if not c or c in ['unnamed']) / len(cols)
        if empty_ratio > 0.3:
            return True

        # Headers look suspiciously like data (very short or numeric-heavy)
        if all(len(c) < 4 or c.replace(',', '').replace('.', '').isdigit() for c in cols if c):
            return True

        return False

    def extract_tables(self, pdf_bytes: bytes) -> Dict[str, Any]:
        """Universal two-stage rescue engine — works on any PDF."""
        tables_list = []
        tmp_path = None
        try:
            with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
                tmp.write(pdf_bytes)
                tmp_path = tmp.name

            # === HYBRID EXTRACTION (your exact stack) ===
            tables_raw = []
            try:
                tables_raw = camelot.read_pdf(tmp_path, flavor="lattice", line_scale=45, pages='all')
            except:
                pass
            if not tables_raw:
                try:
                    tables_raw = camelot.read_pdf(tmp_path, flavor="stream", pages='all')
                except:
                    pass
            if not tables_raw:
                with pdfplumber.open(tmp_path) as pdf:
                    for page in pdf.pages:
                        table = page.extract_table()
                        if table:
                            tables_raw.append(type('obj', (object,), {'df': pd.DataFrame(table), 'page': page.page_number})())

            for idx, t in enumerate(tables_raw):
                df = getattr(t, 'df', pd.DataFrame(t))
                if df.empty:
                    continue
                page_num = getattr(t, 'page', 1)
                raw_csv = df.to_csv(index=False)

                # Stage 1
                resp = self.client.messages.create(
                    model="claude-sonnet-4-6",
                    max_tokens=4000,
                    temperature=0.0,
                    system=self.PERFECTION_PROMPT,
                    messages=[{"role": "user", "content": f"Raw table:\n{raw_csv}"}]
                )
                cleaned = self.extract_json_safe(resp.content[0].text)
                df_clean = pd.read_csv(BytesIO(cleaned.get("csv", raw_csv).encode())) if cleaned.get("csv") else df
                df_clean = self.final_polish(df_clean)
                df_clean = self.handle_merged_cells(df_clean)

                confidence = cleaned.get("confidence", 0.85)

                # Stage 2 — now 100% universal
                if self._needs_rescue(df_clean, confidence):
                    page_text = self._get_full_page_context(tmp_path, [page_num])
                    rescue_input = f"Full page text:\n{page_text}\n\nRaw table:\n{raw_csv}"
                    rescue_resp = self.client.messages.create(
                        model="claude-sonnet-4-6",
                        max_tokens=4000,
                        temperature=0.0,
                        system=self.RESCUE_PROMPT,
                        messages=[{"role": "user", "content": rescue_input}]
                    )
                    cleaned = self.extract_json_safe(rescue_resp.content[0].text)
                    if cleaned.get("csv"):
                        df_clean = pd.read_csv(BytesIO(cleaned["csv"].encode()))
                        df_clean = self.final_polish(df_clean)
                        df_clean = self.handle_merged_cells(df_clean)

                tables_list.append({
                    "table_id": idx + 1,
                    "csv": df_clean.to_csv(index=False),
                    "json": df_clean.to_dict("records"),
                    "confidence": cleaned.get("confidence", 0.92),
                    "page_numbers": [page_num]
                })

            return {
                "success": True,
                "tables": tables_list,
                "message": "Universal extraction complete (two-stage rescue)"
            }

        finally:
            if tmp_path and os.path.exists(tmp_path):
                os.unlink(tmp_path)
