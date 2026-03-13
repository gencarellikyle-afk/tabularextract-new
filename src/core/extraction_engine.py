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

app = FastAPI(title="TabularExtract")

client = Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

# Store last extraction for ZIP download
last_tables = None

# === PERFECT UNIVERSAL PROMPT ===
PERFECTION_PROMPT = """You are the world's #1 PDF table extraction expert. Turn this raw table into perfect Excel-ready CSV + JSON.

STRICT RULES:
- Use ONLY the exact printed headers.
- Repeat section names in every row for hierarchy.
- Put full text in first column for merged cells.
- Convert symbols: ☒→No, ✓→Yes.
- Delete ALL placeholders forever.
- Keep commas in numbers.
- Output ONLY this JSON format:
{"csv": "header1,header2\\nvalue1,value2\\n...", "json": [{"col1":"value"}], "confidence": 0.99}"""

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
    new_cols = [re.sub(r'Column header \(TH\)|Row header \(TH\)|Data cell \(TD\)|\(TH\)|\(TD\)|Unnamed: \d+|Column_\d+', '', str(col).strip(), flags=re.IGNORECASE) or "Column" for col in df.columns]
    df.columns = new_cols
    df = df.replace(['', 'nan', 'NaN', 'None'], '').fillna('')
    return df

# === BEAUTIFUL LANDING PAGE ===
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
      <div class="mx-auto w-16 h-16 mb-6 text-zinc-400">
        <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M7 16a4 4 0 01-.88-7.903 5 5 0 0110.025 1.65L12 13l-.354-.354a2 2 0 01-.293-.293L12 10" />
        </svg>
      </div>
      <p class="text-2xl font-semibold mb-2">Drop your PDF here</p>
      <p class="text-zinc-400">or click to choose a file</p>
    </div>

    <div id="loading" class="hidden text-center mt-12">
      <div class="animate-spin w-16 h-16 border-4 border-emerald-600 border-t-transparent rounded-full mx-auto"></div>
      <p class="mt-6 text-xl">Extracting perfect tables...</p>
    </div>

    <div id="results" class="mt-12"></div>
  </div>

  <script>
    const uploadArea = document.getElementById('uploadArea');
    const fileInput = document.getElementById('pdf');

    uploadArea.addEventListener('click', () => fileInput.click());

    fileInput.addEventListener('change', async () => {
      const file = fileInput.files[0];
      if (!file) return;

      uploadArea.classList.add('hidden');
      document.getElementById('loading').classList.remove('hidden');

      const formData = new FormData();
      formData.append('file', file);

      try {
        const res = await fetch('/upload', { method: 'POST', body: formData });
        const data = await res.json();

        let html = `<h2 class="text-4xl font-bold mb-10 text-center">Your ${data.tables.length} Tables</h2>`;

        data.tables.forEach(table => {
          const blob = new Blob([table.csv], { type: 'text/csv' });
          const url = URL.createObjectURL(blob);
          html += `
            <div class="bg-zinc-900 rounded-3xl p-8 mb-10">
              <div class="flex justify-between mb-6">
                <p class="text-2xl">Table ${table.table_id} — Page ${table.page_numbers}</p>
                <a href="${url}" download="table-${table.table_id}.csv" 
                   class="bg-emerald-600 hover:bg-emerald-700 px-10 py-4 rounded-2xl font-semibold text-lg">Download CSV</a>
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
    });
  </script>
</body>
</html>
"""

@app.post("/upload")
async def upload(file: UploadFile = File(...)):
    global last_tables
    content = await file.read()
    with open("temp.pdf", "wb") as f:
        f.write(content)

    tables = []
    try:
        tables_list = camelot.read_pdf("temp.pdf", flavor="lattice", line_scale=45, pages='all')
        if len(tables_list) == 0:
            tables_list = camelot.read_pdf("temp.pdf", flavor="stream", pages='all')
        if len(tables_list) == 0:
            with pdfplumber.open("temp.pdf") as pdf:
                for page in pdf.pages:
                    table = page.extract_table()
                    if table:
                        tables_list.append(type('obj', (object,), {'df': pd.DataFrame(table), 'page': page.page_number})())

        for i, t in enumerate(tables_list):
            df = t.df if hasattr(t, 'df') else pd.DataFrame(t)
            raw_csv = df.to_csv(index=False)

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

            tables.append({
                "table_id": i+1,
                "csv": df_clean.to_csv(index=False),
                "json": df_clean.to_dict("records"),
                "confidence": cleaned["confidence"],
                "page_numbers": [getattr(t, 'page', 1)]
            })
    except Exception as e:
        return JSONResponse({"success": False, "error": str(e)})

    last_tables = tables
    return {"success": True, "tables": tables, "message": "Universal extraction complete"}

@app.get("/download-all")
async def download_all():
    global last_tables
    if not last_tables or len(last_tables) == 0:
        return JSONResponse({"message": "No tables to download. Upload a PDF first."})

    zip_buffer = BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
        for table in last_tables:
            filename = f"Table_{table['table_id']}_Page_{table['page_numbers'][0]}.csv"
            zip_file.writestr(filename, table["csv"])

    zip_buffer.seek(0)
    return StreamingResponse(
        zip_buffer,
        media_type="application/zip",
        headers={"Content-Disposition": "attachment; filename=all_tables.zip"}
    )

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
