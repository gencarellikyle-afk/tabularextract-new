# v10.2: Fixed 'DataFrame' has no attribute 'str' by safe string conversion

import os, io, re, json, zipfile, tempfile, logging
from pathlib import Path
import pandas as pd
import pdfplumber
import camelot
import anthropic
from fastapi import FastAPI, File, UploadFile, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("tabularextract")

app = FastAPI(title="tabularextract")

# ── Static files ────────────────────────────────────────────────────────────────
@app.get("/", response_class=HTMLResponse)
async def root():
    return Path("index.html").read_text()

@app.get("/privacy", response_class=HTMLResponse)
async def privacy():
    return Path("privacy.html").read_text()

# ── Constants ───────────────────────────────────────────────────────────────────
PLACEHOLDER_RE = re.compile(
    r"^\s*(col(umn)?[_\s]*\d+|th\b|header|column\s+header.*|<th>|unnamed[_:\s]*\d*|field\s*\d*)\s*$",
    re.IGNORECASE,
)
YEAR_RE = re.compile(r"^(19|20)\d{2}$")
NUMBER_RE = re.compile(r"^-?[\$£€]?[\d,]+\.?\d*%?$")
EMPTY_RE = re.compile(r"^\s*$")
AI_CLIENT = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

# ── Header quality scoring ───────────────────────────────────────────────────────
def _header_quality(row: list[str]) -> float:
    """0.0 = all placeholders/empty, 1.0 = all real text headers."""
    if not row:
        return 0.0
    scores = []
    for cell in row:
        c = str(cell).strip()
        if EMPTY_RE.match(c):
            scores.append(0.0)
        elif PLACEHOLDER_RE.match(c):
            scores.append(0.0)
        elif NUMBER_RE.match(c):
            scores.append(0.3)
        else:
            scores.append(1.0)
    return sum(scores) / len(scores)

def _is_data_row(row: list[str]) -> bool:
    """True if row looks like data (mostly numbers/money) not a header."""
    if not row:
        return False
    numeric = sum(1 for c in row if NUMBER_RE.match(str(c).strip()) or EMPTY_RE.match(str(c).strip()))
    return numeric / len(row) >= 0.6

# ── DataFrame cleaning ───────────────────────────────────────────────────────────
def _clean_df(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize whitespace, drop all-empty rows/cols."""
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    
    # Safe cleaning - convert to string first, handle non-string columns
    for col in df.columns:
        df[col] = df[col].astype(str).replace(['nan', 'None', 'nan'], '')
        df[col] = df[col].str.strip().str.replace(r"\s+", " ", regex=True).fillna('')
    
    df.replace("nan", "", inplace=True)
    df = df.loc[~(df == "").all(axis=1)]
    df = df.loc[:, ~(df == "").all(axis=0)]
    return df

def _promote_first_row_if_needed(df: pd.DataFrame) -> pd.DataFrame:
    """If current headers are placeholder/generic, try promoting first data row."""
    if df.empty:
        return df
    hq = _header_quality(list(df.columns))
    if hq >= 0.5:
        return df
    candidate = list(df.iloc[0])
    cq = _header_quality(candidate)
    if cq > hq and not _is_data_row(candidate):
        df.columns = [str(c).strip() if str(c).strip() else f"Col_{i+1}"
                      for i, c in enumerate(candidate)]
        df = df.iloc[1:].reset_index(drop=True)
    return df

def _deduplicate_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Make column names unique."""
    seen: dict[str, int] = {}
    new_cols = []
    for c in df.columns:
        if c in seen:
            seen[c] += 1
            new_cols.append(f"{c}_{seen[c]}")
        else:
            seen[c] = 0
            new_cols.append(c)
    df.columns = new_cols
    return df

def _handle_merged_headers(df: pd.DataFrame) -> pd.DataFrame:
    """Forward-fill empty header cells (common in merged-cell tables)."""
    cols = list(df.columns)
    filled = []
    last = "Col"
    for i, c in enumerate(cols):
        if EMPTY_RE.match(str(c)) or PLACEHOLDER_RE.match(str(c)):
            filled.append(f"{last}_cont_{i}")
        else:
            last = c
            filled.append(c)
    df.columns = filled
    return df

# ── AI header repair (only called when quality < 0.4) ───────────────────────────
def _ai_repair_headers(df: pd.DataFrame, context: str = "") -> pd.DataFrame:
    """Ask Claude to infer real headers from first few rows. Strict JSON response."""
    try:
        preview_rows = df.head(5).values.tolist()
        current_headers = list(df.columns)
        prompt = (
            "You are analyzing a table extracted from a PDF. "
            f"Current column headers (may be wrong/generic): {json.dumps(current_headers)}\n"
            f"First data rows: {json.dumps(preview_rows)}\n"
            f"Context hint: {context}\n"
            "Return ONLY a JSON array of the correct column header strings, one per column, "
            f"exactly {len(current_headers)} elements. Use ONLY text visible in the data. "
            "If you cannot determine a header, use an empty string. No markdown, no explanation."
        )
        resp = AI_CLIENT.messages.create(
            model="claude-3-5-sonnet-20241022",
            max_tokens=300,
            temperature=0,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = resp.content[0].text.strip()
        raw = re.sub(r"```[a-z]*", "", raw).strip().strip("`")
        headers = json.loads(raw)
        if isinstance(headers, list) and len(headers) == len(df.columns):
            df.columns = [str(h).strip() if str(h).strip() else f"Col_{i+1}"
                          for i, h in enumerate(headers)]
    except Exception as e:
        log.warning(f"AI header repair failed: {e}")
    return df

# ── Per-page extraction strategies ──────────────────────────────────────────────
def _camelot_lattice(pdf_path: str, pages: str) -> list[pd.DataFrame]:
    try:
        tables = camelot.read_pdf(pdf_path, pages=pages, flavor="lattice", line_scale=45)
        return [t.df for t in tables if t.df.shape[0] >= 2 and t.df.shape[1] >= 1]
    except Exception as e:
        log.debug(f"camelot lattice failed p{pages}: {e}")
        return []

def _camelot_stream(pdf_path: str, pages: str) -> list[pd.DataFrame]:
    try:
        tables = camelot.read_pdf(pdf_path, pages=pages, flavor="stream",
                                  edge_tol=50, row_tol=10)
        return [t.df for t in tables if t.df.shape[0] >= 2 and t.df.shape[1] >= 1]
    except Exception as e:
        log.debug(f"camelot stream failed p{pages}: {e}")
        return []

def _pdfplumber_extract(pdf_path: str, page_num: int) -> list[pd.DataFrame]:
    try:
        with pdfplumber.open(pdf_path) as pdf:
            if page_num - 1 >= len(pdf.pages):
                return []
            page = pdf.pages[page_num - 1]
            tables = page.extract_tables()
            results = []
            for t in tables:
                if not t or len(t) < 2:
                    continue
                df = pd.DataFrame(t[1:], columns=t[0])
                results.append(df)
            return results
    except Exception as e:
        log.debug(f"pdfplumber failed p{page_num}: {e}")
        return []

def _ocr_page(pdf_path: str, page_num: int) -> list[pd.DataFrame]:
    """Last resort: render page to image and OCR for tables."""
    try:
        import pytesseract
        from PIL import Image
        with pdfplumber.open(pdf_path) as pdf:
            if page_num - 1 >= len(pdf.pages):
                return []
            page = pdf.pages[page_num - 1]
            img = page.to_image(resolution=200).original
            text = pytesseract.image_to_string(img, config="--psm 6")
            lines = [l for l in text.splitlines() if l.strip()]
            if len(lines) < 2:
                return []
            rows = [re.split(r"\s{2,}|\t", l.strip()) for l in lines]
            max_cols = max(len(r) for r in rows)
            rows = [r + [""] * (max_cols - len(r)) for r in rows]
            df = pd.DataFrame(rows[1:], columns=rows[0])
            return [df] if df.shape[0] >= 2 else []
    except Exception as e:
        log.debug(f"OCR failed p{page_num}: {e}")
        return []

# ── Quality-gated multi-pass per page ───────────────────────────────────────────
def _best_tables_for_page(pdf_path: str, page_num: int) -> list[tuple[pd.DataFrame, float]]:
    """
    Returns list of (df, quality_score) for a single page.
    Tries strategies in order, accepts first batch with avg quality >= 0.45.
    """
    page_str = str(page_num)

    for strategy_name, strategy_fn in [
        ("lattice", lambda: _camelot_lattice(pdf_path, page_str)),
        ("stream", lambda: _camelot_stream(pdf_path, page_str)),
        ("pdfplumber", lambda: _pdfplumber_extract(pdf_path, page_num)),
        ("ocr", lambda: _ocr_page(pdf_path, page_num)),
    ]:
        raw_dfs = strategy_fn()
        if not raw_dfs:
            continue

        processed = []
        for df in raw_dfs:
            df = _clean_df(df)
            if df.empty or df.shape[0] < 1:
                continue
            df = _promote_first_row_if_needed(df)
            df = _handle_merged_headers(df)
            df = _deduplicate_columns(df)
            df = _clean_df(df)
            hq = _header_quality(list(df.columns))
            processed.append((df, hq))

        if not processed:
            continue

        avg_q = sum(q for _, q in processed) / len(processed)
        log.info(f"  Page {page_num} {strategy_name}: {len(processed)} tables, avg_q={avg_q:.2f}")

        if avg_q >= 0.45 or strategy_name in ("pdfplumber", "ocr"):
            final = []
            for df, q in processed:
                if q < 0.4:
                    df = _ai_repair_headers(df, context=f"page {page_num}")
                    q = _header_quality(list(df.columns))
                final.append((df, q))
            return final

    return []

# ── Cross-page continuation detection ───────────────────────────────────────────
def _tables_are_continuation(df_a: pd.DataFrame, df_b: pd.DataFrame) -> bool:
    """True if df_b looks like a continuation of df_a (same columns, df_b has no real header)."""
    if list(df_a.columns) == list(df_b.columns):
        return True
    if df_b.shape[1] == df_a.shape[1]:
        if _header_quality(list(df_b.columns)) < 0.3:
            return True
    return False

# ── Main extraction pipeline ─────────────────────────────────────────────────────
def extract_tables(pdf_path: str) -> list[dict]:
    with pdfplumber.open(pdf_path) as pdf:
        n_pages = len(pdf.pages)

    log.info(f"Extracting from {n_pages}-page PDF")
    all_tables: list[tuple[pd.DataFrame, int, float]] = []

    for page_num in range(1, n_pages + 1):
        for df, q in _best_tables_for_page(pdf_path, page_num):
            all_tables.append((df, page_num, q))

    # Merge cross-page continuations
    merged: list[tuple[pd.DataFrame, list[int], float]] = []
    for df, page, q in all_tables:
        if (merged and
                _tables_are_continuation(merged[-1][0], df) and
                page == merged[-1][1][-1] + 1):
            prev_df, pages, prev_q = merged[-1]
            if _header_quality(list(df.columns)) > _header_quality(list(prev_df.columns)):
                prev_df.columns = df.columns
            combined = pd.concat([prev_df, df], ignore_index=True)
            merged[-1] = (_clean_df(combined), pages + [page], max(q, prev_q))
        else:
            merged.append((df, [page], q))

    # Build output
    results = []
    for i, (df, pages, q) in enumerate(merged):
        df = _deduplicate_columns(_clean_df(df))
        if df.empty or df.shape[0] < 1 or df.shape[1] < 1:
            continue
        try:
            records = df.to_dict(orient="records")
        except Exception:
            records = []
        results.append({
            "table_id": f"table_{i+1}",
            "csv": df.to_csv(index=False),
            "json": records,
            "confidence": round(q, 3),
            "page_numbers": pages,
        })

    log.info(f"Extracted {len(results)} tables total")
    return results

# ── FastAPI endpoints ─────────────────────────────────────────────────────────────
@app.post("/extract")
async def extract(file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".pdf"):
        raise HTTPException(400, "Only PDF files are supported.")
    contents = await file.read()
    if len(contents) > 50 * 1024 * 1024:
        raise HTTPException(413, "File too large (max 50MB).")
    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
        tmp.write(contents)
        tmp_path = tmp.name
    try:
        tables = extract_tables(tmp_path)
    except Exception as e:
        log.error(f"Extraction error: {e}", exc_info=True)
        raise HTTPException(500, f"Extraction failed: {str(e)}")
    finally:
        try:
            os.unlink(tmp_path)
        except Exception:
            pass
    return {"success": True, "tables": tables}

@app.post("/download-all")
async def download_all(request: Request):
    body = await request.json()
    tables = body.get("tables", [])
    if not tables:
        raise HTTPException(400, "No tables provided.")
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for t in tables:
            zf.writestr(f"{t.get('table_id', 'table')}.csv", t.get("csv", ""))
        zf.writestr(
            "full_analysis_data.json",
            json.dumps({"tables": tables}, indent=2, ensure_ascii=False),
        )
    buf.seek(0)
    return StreamingResponse(
        buf,
        media_type="application/zip",
        headers={"Content-Disposition": "attachment; filename=tables.zip"},
    )

@app.get("/health")
async def health():
    return {"status": "ok"}
