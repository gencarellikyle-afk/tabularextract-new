from fastapi import FastAPI, UploadFile, File
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
import uvicorn
import os
import re
import json
import pandas as pd
from io import BytesIO
import pdfplumber
import camelot
from anthropic import Anthropic
import zipfile
import tempfile
import pytesseract
from PIL import Image
from typing import Dict, List, Any

app = FastAPI(title="TabularExtract")
last_tables = None

class TableExtractionEngine:
    def __init__(self):
        self.client = Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
        
        self.PERFECTION_PROMPT = """You are the world's #1 PDF table extraction expert. Turn this raw table into perfect Excel-ready CSV + JSON.
STRICT RULES (NEVER break these):
- Use ONLY the exact printed headers from the document. NEVER output Column_0, Column_, Row header (TH), Data cell (TD), .1, .2 or any placeholder.
- NEVER combine multiple tables in one output.
- Repeat section names in every row for hierarchy.
- For merged cells: put full text in LEFTMOST column only.
- Convert symbols: ☒→No, ✓→Yes.
- Keep commas in numbers.
- Output ONLY this JSON: {"csv": "...", "json": [...], "confidence": 0.99}"""

        self.RESCUE_PROMPT = """You have the FULL PAGE TEXT + raw table. Reconstruct using ONLY exact printed headers visible on the page. 
NEVER combine multiple tables. Fix duplicates, shifts, merged cells perfectly. 
NEVER use Column_0, Column_, TH, TD, .1, .2 or any placeholder. Output ONLY this JSON: {"csv": "...", "json": [...], "confidence": 0.99}"""

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
        if df.empty:
            return df
        new_cols = []
        for i, col in enumerate(df.columns):
            col_str = str(col).strip()
            cleaned = re.sub(r'Column header \(TH\)|Row header \(TH\)|Data cell \(TD\)|\(TH\)|\(TD\)|Unnamed: \d+|Column_\d+|Column \d+|\.1|\.2', '', col_str, flags=re.IGNORECASE)
            cleaned = cleaned.strip()
            new_cols.append(cleaned if cleaned else f"Column_{i}")
        df.columns = new_cols
        df = df.replace(['', 'nan', 'NaN', 'None'], '').fillna('')
        return df

    def csv_repair(self, csv_str: str) -> str:
        csv_str = re.sub(r'"(\d+),(\d+)"', r'\1\2', csv_str)
        csv_str = re.sub(r'""(\d+),(\d+)""', r'"\1\2"', csv_str)
        return csv_str

    def local_header_repair(self, df: pd.DataFrame, page_text: str) -> pd.DataFrame:
        if df.empty or not page_text:
            return df
        cols = [str(c).strip() for c in df.columns]
        bad_patterns = ['column_', '(th)', '(td)', '.1', '.2', '']
        page_lines = [line.strip() for line in page_text.split('\n') if len(line.strip()) > 3]
        for i, col in enumerate(cols):
            if any(p in col.lower() for p in bad_patterns):
                for line in page_lines[:20]:
                    match = re.search(r'^([A-Za-z][A-Za-z0-9\s£$%()/\-]+)', line)
                    if match and len(match.group(1).strip()) > 3 and not any(p in match.group(1).lower() for p in bad_patterns):
                        cols[i] = match.group(1).strip()
                        break
        seen = {}
        final_cols = []
        for col in cols:
            if col and col not in seen:
                seen[col] = True
                final_cols.append(col)
        df = df.iloc[:, :len(final_cols)]
        df.columns = final_cols
        return df

    def ocr_on_crop(self, pdf_path: str, page_num: int, bbox: tuple) -> pd.DataFrame:
        """Radical targeted OCR: crop exact camelot bounding box and OCR only that region."""
        try:
            with pdfplumber.open(pdf_path) as pdf:
                page = pdf.pages[page_num - 1]
                # Crop to bbox (camelot bbox is (x0, y0, x1, y1))
                im = page.to_image(resolution=400).original.crop(bbox)
                text = pytesseract.image_to_string(im, config='--psm 6')
                lines = [line.strip() for line in text.split('\n') if line.strip()]
                if not lines:
                    return pd.DataFrame()
                data = []
                for line in lines:
                    if re.search(r'\d', line) or len(line.split()) > 1:
                        data.append(re.split(r'\s{2,}', line.strip()))
                if data:
                    max_cols = max(len(row) for row in data)
                    data = [row + [''] * (max_cols - len(row)) for row in data]
                    return pd.DataFrame(data[1:], columns=data[0])
        except:
            pass
        return pd.DataFrame()

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
        if confidence < 0.92 or df.empty or len(df.columns) == 0:
            return True
        cols = [str(c).strip().lower() for c in df.columns]
        if any(c.startswith('column_') or '(th)' in c or '(td)' in c or c in ['', 'unnamed', '.1', '.2'] for c in cols):
            return True
        if len(cols) > 0 and (cols[0].replace(',', '').replace('.', '').isdigit() or any(x.isdigit() for x in cols[0].split())):
            return True
        return False

    def extract_tables(self, pdf_bytes: bytes) -> Dict[str, Any]:
        tables_list = []
        tmp_path = None
        try:
            with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
                tmp.write(pdf_bytes)
                tmp_path = tmp.name

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
                df = getattr(t, 'df', None)
                if df is None:
                    try:
                        df = pd.DataFrame(t)
                    except Exception:
                        df = pd.DataFrame()
                if df.empty:
                    continue

                page_num = getattr(t, 'page', 1)
                raw_csv = df.to_csv(index=False)
                page_text = self._get_full_page_context(tmp_path, [page_num])

                # Hybrid radical architecture: camelot for boundary detection only
                # If table has bad headers, crop the exact bbox and OCR only that region
                bad_header_ratio = sum(1 for c in df.columns if any(p in str(c).lower() for p in ['column_', 'th', 'td', '.1', '.2', ''])) / max(1, len(df.columns))
                if bad_header_ratio > 0.2 or len(df) < 4 or page_num == 1:
                    # Use camelot bbox for precise crop (camelot tables have .bounding_box)
                    bbox = getattr(t, 'bounding_box', None)
                    if bbox:
                        # bbox is (x0, y0, x1, y1) in points — convert to pixels
                        ocr_df = self.ocr_on_crop(tmp_path, page_num, (int(bbox[0]), int(bbox[1]), int(bbox[2]), int(bbox[3])))
                        if not ocr_df.empty:
                            df = ocr_df
                            print(f"DEBUG: Targeted OCR crop activated for table {idx+1} on page {page_num}")

                # Stage 1
                resp = self.client.messages.create(
                    model="claude-sonnet-4-6",
                    max_tokens=4000,
                    temperature=0.0,
                    system=self.PERFECTION_PROMPT,
                    messages=[{"role": "user", "content": f"Raw table:\n{raw_csv}"}]
                )
                cleaned = self.extract_json_safe(resp.content[0].text)
                
                csv_str = self.csv_repair(cleaned.get("csv", raw_csv))
                if csv_str and csv_str.strip():
                    try:
                        df_clean = pd.read_csv(BytesIO(csv_str.encode()))
                    except Exception:
                        df_clean = df
                else:
                    df_clean = df
                
                df_clean = self.final_polish(df_clean)
                df_clean = self.handle_merged_cells(df_clean)
                df_clean = self.local_header_repair(df_clean, page_text)

                confidence = cleaned.get("confidence", 0.85)

                # Stage 2 Rescue
                if self._needs_rescue(df_clean, confidence):
                    rescue_input = f"Full page text:\n{page_text}\n\nRaw table:\n{raw_csv}"
                    rescue_resp = self.client.messages.create(
                        model="claude-sonnet-4-6",
                        max_tokens=4000,
                        temperature=0.0,
                        system=self.RESCUE_PROMPT,
                        messages=[{"role": "user", "content": rescue_input}]
                    )
                    cleaned = self.extract_json_safe(rescue_resp.content[0].text)
                    
                    csv_str = self.csv_repair(cleaned.get("csv", ""))
                    if csv_str and csv_str.strip():
                        try:
                            df_clean = pd.read_csv(BytesIO(csv_str.encode()))
                        except Exception:
                            pass
                    df_clean = self.final_polish(df_clean)
                    df_clean = self.handle_merged_cells(df_clean)
                    df_clean = self.local_header_repair(df_clean, page_text)

                # Final guardrail
                df_clean = self.final_polish(df_clean)
                df_clean = self.local_header_repair(df_clean, page_text)

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
                "message": "Universal extraction complete (hybrid camelot detection + targeted per-table OCR crop)"
            }
        finally:
            if tmp_path and os.path.exists(tmp_path):
                os.unlink(tmp_path)

@app.get("/", response_class=HTMLResponse)
async def home():
    return """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>TabularExtract - Perfect Tables from Any PDF</title>
  <script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="bg-zinc-950 text-white min-h-screen">
  <div class="max-w-5xl mx-auto p-8">
    <h1 class="text-6xl font-bold text-center mb-4">TabularExtract</h1>
    <p class="text-2xl text-zinc-400 text-center mb-12">Upload any PDF — get perfect tables instantly</p>
    <div id="uploadArea" class="bg-zinc-900 border-2 border-dashed border-zinc-700 rounded-3xl p-16 text-center cursor-pointer">
      <input type="file" id="pdf" accept="application/pdf" class="hidden">
      <div class="mx-auto w-16 h-16 mb-6 text-zinc-400">📤</div>
      <p class="text-2xl font-semibold mb-2">Drop your PDF here</p>
      <p class="text-zinc-400">or click to choose a file</p>
    </div>
    <button id="extractBtn" onclick="startExtraction()" class="mt-8 w-full bg-emerald-600 hover:bg-emerald-700 text-white px-12 py-5 rounded-2xl font-semibold text-2xl hidden">
      Extract Tables Now
    </button>
    <div id="loading" class="hidden text-center mt-12">
      <div class="animate-spin w-16 h-16 border-4 border-emerald-600 border-t-transparent rounded-full mx-auto"></div>
      <p class="mt-6 text-xl">Extracting perfect tables...</p>
    </div>
    <div id="results" class="mt-12"></div>
  </div>
  <script>
    let selectedFile = null;
    let fullData = null;
    const uploadArea = document.getElementById('uploadArea');
    const fileInput = document.getElementById('pdf');
    const extractBtn = document.getElementById('extractBtn');
    uploadArea.addEventListener('click', () => fileInput.click());
    fileInput.addEventListener('change', (e) => {
      selectedFile = e.target.files[0];
      if (selectedFile) {
        extractBtn.classList.remove('hidden');
        extractBtn.textContent = `Extract Tables from ${selectedFile.name}`;
      }
    });
    async function startExtraction() {
      if (!selectedFile) return;
      uploadArea.classList.add('hidden');
      extractBtn.classList.add('hidden');
      document.getElementById('loading').classList.remove('hidden');
      const formData = new FormData();
      formData.append('file', selectedFile);
      try {
        const res = await fetch('/upload', { method: 'POST', body: formData });
        const data = await res.json();
        if (!data.success) {
          document.getElementById('results').innerHTML = `<p class="text-red-500 text-center text-2xl">Error: ${data.error || 'Unknown error'}</p>`;
          return;
        }
        fullData = data;
        console.log("✅ FULL EXTRACTION DATA FOR QUALITY ANALYSIS:", JSON.stringify(data, null, 2));
        let html = `<h2 class="text-4xl font-bold mb-8 text-center">Your ${data.tables.length} Tables</h2>`;
        html += `<div class="text-center mb-10">
          <button onclick="downloadAnalysisJSON()" class="bg-blue-600 hover:bg-blue-700 px-12 py-5 rounded-2xl font-semibold text-xl">📥 Download Full Analysis Data (JSON)</button>
        </div>`;
        data.tables.forEach(table => {
          const blob = new Blob([table.csv], { type: 'text/csv' });
          const url = URL.createObjectURL(blob);
          html += `
            <div class="bg-zinc-900 rounded-3xl p-8 mb-10">
              <div class="flex justify-between items-center mb-6">
                <p class="text-2xl">Table ${table.table_id} — Page ${table.page_numbers}</p>
                <a href="${url}" download="table-${table.table_id}.csv" class="bg-emerald-600 hover:bg-emerald-700 px-10 py-4 rounded-2xl font-semibold text-lg">Download CSV</a>
              </div>
            </div>`;
        });
        html += `<div class="text-center mt-12">
          <a href="/download-all" class="bg-white text-black px-12 py-5 rounded-2xl font-semibold text-2xl">Download All Tables as ZIP</a>
        </div>`;
        document.getElementById('results').innerHTML = html;
      } catch (e) {
        document.getElementById('results').innerHTML = `<p class="text-red-500 text-center text-2xl">Error: ${e.message}</p>`;
      } finally {
        document.getElementById('loading').classList.add('hidden');
      }
    }
    function downloadAnalysisJSON() {
      if (!fullData) return;
      const blob = new Blob([JSON.stringify(fullData, null, 2)], { type: 'application/json' });
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = 'full_analysis_data.json';
      a.click();
    }
  </script>
</body>
</html>
"""

@app.post("/upload")
async def upload(file: UploadFile = File(...)):
    global last_tables
    try:
        content = await file.read()
        engine = TableExtractionEngine()
        result = engine.extract_tables(content)
        last_tables = result.get("tables", [])
        return result
    except Exception as e:
        return JSONResponse({"success": False, "error": str(e)})

@app.get("/download-all")
async def download_all():
    global last_tables
    if not last_tables:
        return JSONResponse({"message": "No tables yet."})
    zip_buffer = BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as z:
        for t in last_tables:
            z.writestr(f"Table_{t['table_id']}_Page_{t['page_numbers'][0]}.csv", t["csv"])
    zip_buffer.seek(0)
    return StreamingResponse(zip_buffer, media_type="application/zip", headers={"Content-Disposition": "attachment; filename=all_tables.zip"})

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
