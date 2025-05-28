raise Exception("Force crash to test Gunicorn load")

from flask import Flask, request, send_file
import fitz  # PyMuPDF
import re
import pandas as pd
import tempfile
import os
from werkzeug.utils import secure_filename

app = Flask(__name__)

def extract_val(pattern, text, default=None):
    match = re.search(pattern, text)
    try:
        return match.group(1).strip() if match else default
    except IndexError:
        return default

def parse_limited_morning_report(pdf_path: str) -> pd.DataFrame:
    doc = fitz.open(pdf_path)
    text_pages = [doc.load_page(p).get_text() for p in range(len(doc))]
    full_text = "\n".join(text_pages)
    reports = re.split(r'(?=Limited Morning Report for \d{2}/\d{2}/\d{4})', full_text)
    reports = [r.strip() for r in reports if "Limited Morning Report for" in r]

    all_data = []

    for report in reports:
        row = {}
        row["Well Name"] = extract_val(r'Well\s+(.*?)\n', report)
        row["Rig"] = extract_val(r'Rig\s+(.*?)\n', report)
        row["Date"] = extract_val(r'Limited Morning Report for\s+(\d{2}/\d{2}/\d{4})', report)
        row["Location"] = extract_val(r'Location\s+(.*?)\n', report)
        row["Objective"] = extract_val(r'Objective\s*:\s*\(?([^)]+)\)?', report)
        row["Foreman Remarks"] = extract_val(r'Foreman Remarks\s*\n(.*?)(?:Page \d+ of \d+|\n\s*Saudi Aramco|mailto:)', report, default="")
        row["DSLTA"] = extract_val(r'DSLTA\s+([0-9,]+)', report)
        row["H2S"] = extract_val(r'H2S\s*[:=]?\s*([0-9.]+)\s*%', report)
        row["GOR"] = extract_val(r'GOR\s*[:=]?\s*([0-9,]+)\s*SCF/BBL', report)
        all_data.append(row)

    return pd.DataFrame(all_data)

@app.route("/")
def home():
    return "DMR Batch Parser is running."

@app.route("/parse-batch", methods=["POST"])
def parse_batch():
    if 'files' not in request.files:
        return "No files uploaded", 400

    files = request.files.getlist("files")
    combined_df = pd.DataFrame()

    for file in files:
        filename = secure_filename(file.filename)
        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
            file.save(tmp.name)
            try:
                df = parse_limited_morning_report(tmp.name)
                combined_df = pd.concat([combined_df, df], ignore_index=True)
            finally:
                os.unlink(tmp.name)

    output_path = "combined_output.xlsx"
    combined_df.to_excel(output_path, index=False, engine="openpyxl")
    return send_file(output_path, as_attachment=True, download_name="DMR_Combined.xlsx", mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
