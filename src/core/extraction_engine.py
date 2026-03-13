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

app = FastAPI(title="TabularExtract")
client = Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
last_tables = None

# === LIGHTWEIGHT MERGED-CELL FIX ===
def handle_merged_cells(df):
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

# === STAGE 1 PROMPT (fast normal tables) ===
PERFECTION_PROMPT = """You are the world's #1 PDF table extraction expert. Turn this raw table into perfect Excel-ready CSV + JSON.
STRICT RULES (NEVER break these):
- Use ONLY the exact printed headers from the document. NEVER output Column_0, Column header, etc.
- Repeat section names in every row for hierarchy.
- For merged cells: put full text in LEFTMOST column only.
- Convert symbols: ☒→No, ✓→Yes.
- Keep commas in numbers.
- Output ONLY this JSON: {"csv": "...", "json": [...], "confidence": 0.99}"""

# === STAGE 2 RESCUE PROMPT (full page context for hard tables) ===
RESCUE_PROMPT = """You have the FULL PAGE TEXT + raw table. Reconstruct using ONLY exact printed headers visible on the page. Fix duplicates, shifts, merged cells perfectly. Never use Column_0 or generic names. Output ONLY this JSON: {"csv": "...", "json": [...], "confidence": 0.99}"""

def extract_json_safe(text):
    text = re.sub(r'[\x00-\x1F\x7F]', '', text)
    match = re.search(r'\{.*\}', text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group(0))
        except:
            pass
    return {"csv": "", "json": [], "confidence": 0.0}

def final_polish(df):
    new_cols = [re.sub(r'Column header \(TH\)|Row header \(TH\)|Data cell \(TD\)|\(TH\)|\(TD\)|Unnamed: \d+|Column_\d+|Column \d+|\.1|Expenditure by function £ million|Expenditure by function £million', '', str(col).strip(), flags=re.IGNORECASE) or f"Column_{i}" for i, col in enumerate(df.columns)]
    df.columns = new_cols
    df = df.replace(['', 'nan', 'NaN', 'None'], '').fillna('')
    return df

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
    tmp_path = None
    try:
        content = await file.read()
        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
            tmp.write(content)
            tmp_path = tmp.name

        # Extract all tables + page text once
        tables_list = []
        page_texts = {}
        try:
            tables_list = camelot.read_pdf(tmp_path, flavor="lattice", line_scale=45, pages='all')
            if len(tables_list) == 0:
                tables_list = camelot.read_pdf(tmp_path, flavor="stream", pages='all')
        except:
            pass
        if len(tables_list) == 0:
            with pdfplumber.open(tmp_path) as pdf:
                for page in pdf.pages:
                    table = page.extract_table()
                    if table:
                        tables_list.append(type('obj', (object,), {'df': pd.DataFrame(table), 'page': page.page_number})())
                    page_texts[page.page_number] = page.extract_text() or ""

        tables = []
        for i, t in enumerate(tables_list):
            df = t.df if hasattr(t, 'df') else pd.DataFrame(t)
            raw_csv = df.to_csv(index=False)
            page_num = getattr(t, 'page', 1)
            page_text = page_texts.get(page_num, "")

            # === STAGE 1: Normal fast pass ===
            resp = client.messages.create(
                model="claude-sonnet-4-6",
                max_tokens=4000,
                temperature=0.0,
                system=PERFECTION_PROMPT,
                messages=[{"role": "user", "content": f"Fix this raw table:\n{raw_csv}"}]
            )
            cleaned = extract_json_safe(resp.content[0].text)
            df_clean = pd.read_csv(BytesIO(cleaned["csv"].encode())) if cleaned["csv"] else df
            df_clean = final_polish(df_clean)
            df_clean = handle_merged_cells(df_clean)

            # === QUALITY GATE → STAGE 2 RESCUE ===
            cols = [str(c).strip().lower() for c in df_clean.columns]
            needs_rescue = (cleaned.get("confidence", 0) < 0.95 or
                            any(c.startswith("column_") or "header" in c for c in cols) or
                            "column_0" in cols)

            if needs_rescue and page_text:
                rescue_input = f"Full page text:\n{page_text}\n\nRaw table CSV:\n{raw_csv}"
                rescue_resp = client.messages.create(
                    model="claude-sonnet-4-6",
                    max_tokens=4000,
                    temperature=0.0,
                    system=RESCUE_PROMPT,
                    messages=[{"role": "user", "content": rescue_input}]
                )
                cleaned = extract_json_safe(rescue_resp.content[0].text)
                df_clean = pd.read_csv(BytesIO(cleaned["csv"].encode())) if cleaned["csv"] else df_clean
                df_clean = final_polish(df_clean)
                df_clean = handle_merged_cells(df_clean)

            tables.append({
                "table_id": i+1,
                "csv": df_clean.to_csv(index=False),
                "json": df_clean.to_dict("records"),
                "confidence": cleaned.get("confidence", 0.99),
                "page_numbers": [page_num]
            })

        last_tables = tables
        return {"success": True, "tables": tables, "message": "Universal extraction complete (two-stage rescue active)"}
    except Exception as e:
        return JSONResponse({"success": False, "error": str(e)})
    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.unlink(tmp_path)

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
