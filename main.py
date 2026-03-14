#!/usr/bin/env python3
"""tabularextract.com — Universal PDF Table Extractor v8.0
Raw-matrix-first: Column_N/TH/TD structurally impossible after raw_to_df()."""

from __future__ import annotations
import asyncio, base64, csv, gc, io, json, logging, os, re, sys
import tempfile, traceback, zipfile
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import anthropic, numpy as np, pandas as pd, pdfplumber
import stripe, uvicorn
from fastapi import FastAPI, File, HTTPException, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

# ── Config ────────────────────────────────────────────────────────────────────
log = logging.getLogger("te")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
STRIPE_SECRET_KEY = os.environ.get("STRIPE_SECRET_KEY", "")
STRIPE_WEBHOOK_SECRET = os.environ.get("STRIPE_WEBHOOK_SECRET", "")
BASE_URL = os.environ.get("BASE_URL", "https://tabularextract.com")
CLAUDE_MODEL = os.environ.get("CLAUDE_MODEL", "claude-sonnet-4-5")
MAX_CLAUDE, CLAUDE_THRESH, MAX_FILE_MB = 10, 0.65, 50

if STRIPE_SECRET_KEY:
    stripe.api_key = STRIPE_SECRET_KEY

app = FastAPI(title="TabularExtract", version="8.0.0")
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)
Path("static").mkdir(exist_ok=True)
try:
    app.mount("/static", StaticFiles(directory="static"), name="static")
except Exception:
    pass

# ── Placeholder detection ─────────────────────────────────────────────────────
_PH_RE = re.compile(
    r"^(column[_\s]?\d*|col[_\s]?\d+|field[_\s]?\d+|unnamed[_:\s]\d*"
    r"|\d+|th\d*|td\d*|var\d+|x\d+|f\d+|_\\d+|nan|none)$",
    re.I,
)

def is_ph(v: Any) -> bool:
    s = str(v).strip() if v is not None else ""
    return not s or s in {"-", "–", "—", "n/a"} or bool(_PH_RE.match(s))

def all_ph(names: List[Any]) -> bool:
    return bool(names) and all(is_ph(n) for n in names)

# ══════════════════════════════════════════════════════════════════════════════
# CORE: raw_to_df — THE SINGLE CHOKEPOINT
# ══════════════════════════════════════════════════════════════════════════════
def _c(v: Any) -> str:
    s = "" if v is None else str(v).strip()
    return "" if s.lower() in {"none", "nan"} else s

def _norm(raw: List[List[Any]]) -> List[List[str]]:
    if not raw:
        return []
    nc = max(len(r) for r in raw)
    return [[_c(r[i]) if i < len(r) else "" for i in range(nc)] for r in raw]

def _dedup_names(names: List[str]) -> List[str]:
    seen: Dict[str, int] = {}
    out = []
    for n in names:
        n = re.sub(r"[\n\r\t|]+", " ", n).strip()
        n = re.sub(r"\s{2,}", " ", n) or "Col"
        if n in seen:
            seen[n] += 1
            out.append(f"{n}_{seen[n]}")
        else:
            seen[n] = 1
            out.append(n)
    return out

def _make_header(row: List[str]) -> List[str]:
    return _dedup_names(
        [v if v and not is_ph(v) else f"Col_{i}" for i, v in enumerate(row)]
    )

def _row_is_header(row: List[str]) -> bool:
    vals = [v for v in row if v]
    if len(vals) < 2:
        return False
    num = sum(1 for v in vals if re.match(r"^-?[\d,.\s]+%?$", v))
    if num / len(vals) > 0.35:
        return False
    if any(len(v) > 80 for v in vals):
        return False
    return sum(1 for v in vals if re.search(r"[a-zA-Z]{2,}", v)) >= 1

def _split_on_empty(rows: List[List[str]]) -> List[List[List[str]]]:
    blocks, cur = [], []
    for row in rows:
        if all(not v for v in row):
            if cur:
                blocks.append(cur)
                cur = []
        else:
            cur.append(row)
    if cur:
        blocks.append(cur)
    return [b for b in blocks if len(b) >= 2]

def _split_col_jump(rows: List[List[str]]) -> List[List[List[str]]]:
    if len(rows) < 4:
        return [rows]
    ne = [sum(1 for v in r if v) for r in rows]
    avg = sum(ne) / len(ne)
    for i in range(2, len(rows) - 1):
        a = sum(ne[:i]) / i
        b = sum(ne[i:]) / (len(rows) - i)
        if abs(a - b) > max(avg * 0.4, 1.5):
            return [rows[:i], rows[i:]]
    return [rows]

def _promote(block: List[List[str]]) -> Tuple[List[str], List[List[str]]]:
    if not block:
        return [], []
    if _row_is_header(block[0]):
        return _make_header(block[0]), block[1:]
    if len(block) >= 2 and sum(1 for v in block[0] if not v) > len(block[0]) * 0.5:
        if _row_is_header(block[1]):
            return _make_header(block[1]), block[2:]
    candidate = [v if v else f"Col_{i}" for i, v in enumerate(block[0])]
    if all_ph(candidate):
        return _make_header(block[0]), block[1:]
    real = [v for v in block[0] if v and not is_ph(v)]
    if len(real) >= max(2, len(block[0]) // 2):
        return _make_header(block[0]), block[1:]
    return _make_header(block[0]), block[1:]

def raw_to_df(raw: List[List[Any]]) -> List[pd.DataFrame]:
    m = _norm(raw)
    if not m:
        return []
    results = []
    for block in (_split_on_empty(m) or [m]):
        for sub in (_split_col_jump(block) or [block]):
            if len(sub) < 2:
                continue
            header, data = _promote(sub)
            if not header or not data:
                continue
            data = [r for r in data if r != header]
            df = pd.DataFrame(data, columns=header)
            df.replace("", np.nan, inplace=True)
            df.dropna(how="all", inplace=True)
            df.dropna(axis=1, how="all", inplace=True)
            df.fillna("", inplace=True)
            df.reset_index(drop=True, inplace=True)
            if not df.empty and len(df.columns) >= 2:
                results.append(df)
    return results

# ── Light post-DataFrame polish ───────────────────────────────────────────────
def _clean_col_names(df: pd.DataFrame) -> pd.DataFrame:
    new = []
    for i, c in enumerate(df.columns):
        s = re.sub(r"[\n\r\t|]+", " ", str(c)).strip()
        s = re.sub(r"\s{2,}", " ", s)
        new.append(s if s and not is_ph(s) else f"Col_{i}")
    df.columns = _dedup_names(new)
    return df

def _ffill_labels(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        s = df[col].astype(str).str.strip()
        non_e = s[~s.isin(["", "nan", "None"])]
        if len(non_e) == 0:
            continue
        if non_e.apply(lambda v: bool(re.match(r"^-?[\d,.\s]+%?$", v))).mean() < 0.5:
            last, filled = None, []
            for v in s:
                if v in ("", "nan", "None", "-", "–"):
                    filled.append(last or v)
                else:
                    last = v
                    filled.append(v)
            df[col] = filled
    return df

def clean(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    if df is None or df.empty:
        return None
    if all_ph(list(df.columns)):
        results = raw_to_df([list(df.columns)] + df.values.tolist())
        if results:
            df = results[0]
    df = _clean_col_names(df)
    df.replace("", np.nan, inplace=True)
    df.dropna(how="all", inplace=True)
    df.dropna(axis=1, how="all", inplace=True)
    df.fillna("", inplace=True)
    df.reset_index(drop=True, inplace=True)
    df = _ffill_labels(df)
    return df if (not df.empty and len(df.columns) >= 2) else None

# ── Confidence score ──────────────────────────────────────────────────────────
def score(df: pd.DataFrame) -> float:
    if df is None or df.empty:
        return 0.0
    cols, nr = list(df.columns), len(df)
    nc = len(cols)
    good_h = sum(1 for c in cols if not is_ph(c) and len(str(c).strip()) > 1)
    col_l = [str(c).strip().lower() for c in cols]
    dup = df.apply(lambda r: [str(v).strip().lower() for v in r] == col_l, axis=1).any()
    ne_c = df.apply(lambda r: sum(1 for v in r if str(v).strip() not in ("", "nan")), axis=1)
    consistency = max(0, 1 - ne_c.std() / max(ne_c.mean(), 1)) if nr > 1 else 1.0
    fill = sum(1 for v in df.values.flatten() if str(v).strip() not in ("", "nan")) / max(df.size, 1)
    valid_n = sum(1 for c in cols if len(str(c).strip()) > 1 and not re.match(r"^\d+$", str(c).strip()))
    return round(min(0.30 * (good_h / max(nc, 1)) + 0.15 * (0 if dup else 1) + 0.15 * consistency + 0.15 * fill + 0.10 * (1 if nc >= 2 and nr >= 1 else 0) + 0.15 * (valid_n / max(nc, 1)), 1.0), 4)

# ── pdfplumber extraction ─────────────────────────────────────────────────────
_SL = dict(vertical_strategy="lines", horizontal_strategy="lines", snap_tolerance=3, join_tolerance=3, edge_min_length=10, min_words_vertical=1, min_words_horizontal=1, intersection_tolerance=3, text_tolerance=3)
_SM = dict(vertical_strategy="lines", horizontal_strategy="text", snap_tolerance=4, join_tolerance=4, edge_min_length=8, min_words_vertical=1, min_words_horizontal=1, intersection_tolerance=4, text_x_tolerance=4, text_y_tolerance=4)
_ST = dict(vertical_strategy="text", horizontal_strategy="text", snap_tolerance=5, join_tolerance=5, edge_min_length=5, min_words_vertical=1, min_words_horizontal=1, intersection_tolerance=5, text_x_tolerance=5, text_y_tolerance=5)

def _bbox_overlap(a: Tuple, b: Tuple) -> float:
    ix0, iy0 = max(a[0], b[0]), max(a[1], b[1])
    ix1, iy1 = min(a[2], b[2]), min(a[3], b[3])
    if ix1 <= ix0 or iy1 <= iy0:
        return 0.0
    return (ix1 - ix0) * (iy1 - iy0) / max((a[2] - a[0]) * (a[3] - a[1]), 1)

def _plumber_page(page: Any, pnum: int) -> List[Dict]:
    out, used = [], []
    for sname, settings in [("lines", _SL), ("mixed", _SM), ("text", _ST)]:
        found = []
        try:
            fts = page.find_tables(settings)
        except Exception:
            continue
        for ft in fts:
            try:
                bbox = ft.bbox
            except Exception:
                bbox = None
            if bbox and any(_bbox_overlap(bbox, u) > 0.4 for u in used):
                continue
            try:
                raw = ft.extract()
            except Exception:
                continue
            if not raw or len(raw) < 2:
                continue
            for df in raw_to_df(raw):
                df2 = clean(df)
                if df2 is None:
                    continue
                found.append({"df": df2, "page_num": pnum, "bbox": bbox, "strategy": sname, "conf": score(df2)})
            if bbox:
                used.append(bbox)
        out.extend(found)
        if found and sname in ("lines", "mixed"):
            break
    return out

def extract_plumber(pdf_path: str) -> Tuple[List[Dict], int]:
    tables, total = [], 0
    try:
        with pdfplumber.open(pdf_path) as pdf:
            total = len(pdf.pages)
            for i, page in enumerate(pdf.pages, 1):
                try:
                    pt = _plumber_page(page, i)
                    tables.extend(pt)
                    if pt:
                        log.info("plumber p%d: %d tables", i, len(pt))
                except Exception as e:
                    log.warning("plumber p%d: %s", i, e)
    except Exception as e:
        log.error("plumber fatal: %s", e)
    return tables, total

# ── Camelot fallback ──────────────────────────────────────────────────────────
def extract_camelot(pdf_path: str, pages: List[int]) -> List[Dict]:
    if not pages:
        return []
    try:
        import camelot as _c
    except ImportError:
        return []
    ps = ",".join(str(p) for p in sorted(set(pages)))
    out = []
    for flavor, kw in [("lattice", {"line_scale": 45, "copy_text": ["v", "h"]}), ("stream", {"edge_tol": 500, "row_tol": 10})]:
        try:
            for ct in _c.read_pdf(pdf_path, pages=ps, flavor=flavor, **kw):
                raw_df = ct.df
                col_names = list(raw_df.columns)
                full_raw = ([col_names] + raw_df.values.tolist()) if all_ph(col_names) else ([list(raw_df.iloc[0])] + raw_df.values.tolist()[1:])
                for df in raw_to_df(full_raw):
                    df2 = clean(df)
                    if df2 is None:
                        continue
                    out.append({"df": df2, "page_num": int(ct.page), "bbox": None, "strategy": f"cam_{flavor}", "conf": score(df2)})
            if out:
                break
        except Exception as e:
            log.warning("camelot %s: %s", flavor, e)
    return out

# ── OCR fallback ──────────────────────────────────────────────────────────────
def extract_ocr(pdf_path: str, pnum: int) -> List[Dict]:
    try:
        import pytesseract
        with pdfplumber.open(pdf_path) as pdf:
            img = pdf.pages[pnum - 1].to_image(resolution=200).original
            lines = [ln for ln in pytesseract.image_to_string(img, config="--psm 6").splitlines() if ln.strip()]
            blocks, cur, prev_nc = [], [], None
            for ln in lines:
                parts = re.split(r"\s{2,}", ln.strip())
                nc = len(parts)
                if nc >= 2:
                    if prev_nc is None or abs(nc - prev_nc) <= 1:
                        cur.append(parts)
                        prev_nc = nc
                    else:
                        if cur:
                            blocks.append(cur)
                        cur = [parts]
                        prev_nc = nc
                else:
                    if cur:
                        blocks.append(cur)
                    cur = []
                    prev_nc = None
            if cur:
                blocks.append(cur)
            out = []
            for b in blocks:
                if len(b) < 2:
                    continue
                for df in raw_to_df(b):
                    df2 = clean(df)
                    if df2 is None:
                        continue
                    out.append({"df": df2, "page_num": pnum, "bbox": None, "strategy": "ocr", "conf": score(df2)})
            return out
    except Exception as e:
        log.warning("ocr p%d: %s", pnum, e)
        return []

# ── Dedup ─────────────────────────────────────────────────────────────────────
def dedup(tables: List[Dict]) -> List[Dict]:
    seen: Dict[str, Dict] = {}
    for t in tables:
        df = t["df"]
        cols = "|".join(str(c)[:12] for c in df.columns[:5])
        r0 = "|".join(str(v)[:8] for v in (df.iloc[0] if len(df) else []))[:40]
        fp = f"{len(df.columns)}x{len(df)}:{cols}:{r0}"
        if fp not in seen or t["conf"] > seen[fp]["conf"]:
            seen[fp] = t
    return sorted(
        seen.values(),
        key=lambda t: (t["page_num"], (t.get("bbox") or (0, 0, 0, 0))[1]),
    )

# ── Claude rescue ─────────────────────────────────────────────────────────────
def _raw_words(pdf_path: str, pnum: int, bbox: Optional[Tuple]) -> str:
    try:
        with pdfplumber.open(pdf_path) as pdf:
            page = pdf.pages[pnum - 1]
            region = page.within_bbox(bbox) if bbox else page
            words = region.extract_words(x_tolerance=3, y_tolerance=3)
            if not words:
                return page.extract_text() or ""
            yb: Dict[int, List] = {}
            for w in words:
                k = round(w["top"] / 8) * 8
                yb.setdefault(k, []).append(w)
            return "\n".join(
                " ".join(w["text"] for w in sorted(yb[k], key=lambda w: w["x0"]))
                for k in sorted(yb)
            )[:2500]
    except Exception:
        return ""

async def claude_rescue(t: Dict, pdf_path: str, label: str) -> pd.DataFrame:
    if not ANTHROPIC_API_KEY:
        return t["df"]
    ctx = _raw_words(pdf_path, t["page_num"], t.get("bbox"))
    if not ctx.strip():
        return t["df"]
    prompt = (
        f"Extract the table from page {t['page_num']}. "
        f"Current broken cols: {list(t['df'].columns)}\n\n"
        f"Raw PDF text (ground truth):\n```\n{ctx}\n```\n\n"
        "Return ONLY a CSV with real descriptive headers on row 1 "
        "(no Column_0/TH/TD/Col_N placeholders, no markdown fences). CSV:"
    )
    try:
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        resp = await asyncio.to_thread(
            client.messages.create,
            model=CLAUDE_MODEL,
            max_tokens=4096,
            messages=[{"role": "user", "content": prompt}],
        )
        csv_txt = re.sub(r"^```[a-zA-Z]*\n?", "", resp.content[0].text.strip(), flags=re.M)
        csv_txt = re.sub(r"\n?```\s*$", "", csv_txt, flags=re.M).strip()
        rows = list(csv.reader(io.StringIO(csv_txt)))
        if len(rows) < 2:
            return t["df"]
        dfs = raw_to_df(rows)
        if not dfs:
            return t["df"]
        new_df = clean(dfs[0])
        if new_df is None:
            return t["df"]
        ns, os_ = score(new_df), score(t["df"])
        log.info("claude %s: %.2f→%.2f cols=%s", label, os_, ns, list(new_df.columns)[:4])
        return new_df if (ns >= os_ or os_ < 0.5) else t["df"]
    except Exception as e:
        log.error("claude %s: %s", label, e)
        return t["df"]

# ── Pipeline ──────────────────────────────────────────────────────────────────
async def run_pipeline(pdf_path: str) -> List[Dict]:
    tables, total_pages = extract_plumber(pdf_path)
    covered = {t["page_num"] for t in tables}

    missing = [p for p in range(1, total_pages + 1) if p not in covered]
    if missing:
        ct = extract_camelot(pdf_path, missing)
        tables.extend(ct)
        covered.update(t["page_num"] for t in ct)

    for p in [pp for pp in range(1, total_pages + 1) if pp not in covered][:5]:
        tables.extend(extract_ocr(pdf_path, p))

    if not tables:
        return []

    cleaned = []
    for t in tables:
        df = clean(t["df"].copy())
        if df is None:
            continue
        t["df"] = df
        t["conf"] = score(df)
        cleaned.append(t)

    unique = dedup(cleaned)

    low = [(i, t) for i, t in enumerate(unique) if t["conf"] < CLAUDE_THRESH][:MAX_CLAUDE]
    if low and ANTHROPIC_API_KEY:
        sem = asyncio.Semaphore(3)

        async def _r(i, t):
            async with sem:
                return i, await claude_rescue(t, pdf_path, f"t{i+1}_p{t['page_num']}")

        for res in await asyncio.gather(*[_r(i, t) for i, t in low], return_exceptions=True):
            if isinstance(res, Exception):
                continue
            i, df = res
            if df is not None and not df.empty:
                unique[i]["df"] = df
                unique[i]["conf"] = score(df)

    final = []
    for t in unique:
        df = clean(t["df"].copy())
        if df is None:
            continue
        t["df"] = df
        t["conf"] = score(df)
        final.append(t)

    log.info("FINAL: %d tables", len(final))
    for i, t in enumerate(final):
        log.info(
            " t%d p%d %s conf=%.2f cols=%s",
            i + 1, t["page_num"], t["df"].shape, t["conf"],
            list(t["df"].columns)[:4],
        )
    return final

# ── Output helpers ────────────────────────────────────────────────────────────
def to_csv(df: pd.DataFrame) -> str:
    b = io.StringIO()
    df.to_csv(b, index=False)
    return b.getvalue()

def to_records(df: pd.DataFrame) -> List[Dict]:
    return df.where(pd.notnull(df), "").to_dict(orient="records")

def build_response(tables: List[Dict]) -> List[Dict]:
    return [
        {
            "table_id": f"table_{i+1}",
            "csv": to_csv(t["df"]),
            "json": to_records(t["df"]),
            "confidence": round(t["conf"], 4),
            "page_numbers": [t["page_num"]],
        }
        for i, t in enumerate(tables)
    ]

def build_zip(resp: List[Dict]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for t in resp:
            zf.writestr(f"{t['table_id']}.csv", t["csv"])
        zf.writestr(
            "full_analysis_data.json",
            json.dumps(
                {
                    "success": True,
                    "total_tables": len(resp),
                    "tables": [
                        {
                            "table_id": t["table_id"],
                            "page_numbers": t["page_numbers"],
                            "confidence": t["confidence"],
                            "columns": list(t["json"][0].keys()) if t["json"] else [],
                        }
                        for t in resp
                    ],
                },
                indent=2,
            ),
        )
    return buf.getvalue()

# ── Upload handler ────────────────────────────────────────────────────────────
async def _handle(file: UploadFile) -> Tuple[List[Dict], bytes]:
    if not (file.filename or "").lower().endswith(".pdf"):
        raise HTTPException(400, "Only PDF files supported.")
    data = await file.read()
    if len(data) < 100:
        raise HTTPException(400, "File too small.")
    if len(data) > MAX_FILE_MB * 1024 * 1024:
        raise HTTPException(400, f"Max {MAX_FILE_MB}MB.")
    tmp = None
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as f:
            f.write(data)
            tmp = f.name
        tables = await run_pipeline(tmp)
        resp = build_response(tables)
        return resp, build_zip(resp)
    finally:
        if tmp:
            try:
                os.unlink(tmp)
            except Exception:
                pass
        gc.collect()

# ── Routes ────────────────────────────────────────────────────────────────────
@app.get("/", response_class=HTMLResponse)
async def root():
    p = Path("index.html")
    return HTMLResponse(p.read_text() if p.exists() else "<h1>TabularExtract</h1>")

@app.get("/privacy", response_class=HTMLResponse)
async def privacy():
    p = Path("privacy.html")
    return HTMLResponse(p.read_text() if p.exists() else "<h1>Privacy</h1>")

@app.get("/health")
async def health():
    return {"status": "ok", "version": "8.0.0"}

@app.post("/extract")
async def extract(file: UploadFile = File(...)):
    try:
        resp, _ = await _handle(file)
        return JSONResponse({"success": True, "tables": resp})
    except HTTPException:
        raise
    except Exception as e:
        log.error(traceback.format_exc())
        raise HTTPException(500, str(e))

@app.post("/extract-with-files")
async def extract_with_files(file: UploadFile = File(...)):
    try:
        resp, zb = await _handle(file)
        return JSONResponse({
            "success": True,
            "tables": resp,
            "zip_base64": base64.b64encode(zb).decode(),
            "total_tables": len(resp),
        })
    except HTTPException:
        raise
    except Exception as e:
        log.error(traceback.format_exc())
        raise HTTPException(500, str(e))

@app.post("/extract-zip")
async def extract_zip(file: UploadFile = File(...)):
    try:
        _, zb = await _handle(file)
        return StreamingResponse(
            io.BytesIO(zb),
            media_type="application/zip",
            headers={"Content-Disposition": "attachment; filename=tables.zip"},
        )
    except HTTPException:
        raise
    except Exception as e:
        log.error(traceback.format_exc())
        raise HTTPException(500, str(e))

@app.post("/stripe-webhook")
async def stripe_webhook(request: Request):
    payload = await request.body()
    sig = request.headers.get("stripe-signature", "")
    try:
        event = stripe.Webhook.construct_event(payload, sig, STRIPE_WEBHOOK_SECRET)
        log.info("stripe %s", event["type"])
        return JSONResponse({"received": True})
    except Exception as e:
        raise HTTPException(400, str(e))

@app.post("/create-checkout-session")
async def create_checkout(request: Request):
    try:
        data = await request.json()
        plan = data.get("plan", "starter")
        prices = {
            "starter": os.environ.get("STRIPE_PRICE_STARTER", ""),
            "pro": os.environ.get("STRIPE_PRICE_PRO", ""),
            "enterprise": os.environ.get("STRIPE_PRICE_ENTERPRISE", ""),
        }
        pid = prices.get(plan, "")
        if not pid:
            raise HTTPException(400, f"Unknown plan: {plan}")
        s = stripe.checkout.Session.create(
            payment_method_types=["card"],
            line_items=[{"price": pid, "quantity": 1}],
            mode="subscription",
            success_url=f"{BASE_URL}/success",
            cancel_url=f"{BASE_URL}/cancel",
        )
        return JSONResponse({"url": s.url})
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))

@app.on_event("startup")
async def startup():
    log.info(
        "TabularExtract v8.0.0 | Claude=%s | Anthropic=%s",
        CLAUDE_MODEL,
        "SET" if ANTHROPIC_API_KEY else "NOT SET",
    )

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=int(os.environ.get("PORT", 8000)),
        reload=False,
        workers=1,
    )
