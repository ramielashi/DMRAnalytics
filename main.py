from flask import Flask, request, send_file
from flask_cors import CORS
import fitz  # PyMuPDF
import re
import pandas as pd
import tempfile
import os
from werkzeug.utils import secure_filename

app = Flask(__name__)
CORS(app)

def extract_val(pattern, text, default=None):
    match = re.search(pattern, text)
    try:
        return match.group(1).strip() if match else default
    except IndexError:
        return default

def extract_summary_operations(report, date, rig, well):
    rows = []
    lines = report.splitlines()
    i = 0
    while i < len(lines):
        if re.match(r"\d{4} - \d{4}", lines[i]):
            time_range = lines[i].strip()
            summary_lines = []
            i += 1
            while i < len(lines) and not re.match(r"\d{4} - \d{4}", lines[i]):
                summary_lines.append(lines[i])
                i += 1
            summary_text = "\n".join(summary_lines).strip()
            time_parts = time_range.split(" - ")
            rows.append({
                "Date": date,
                "Rig": rig,
                "Well": well,
                "From - To": time_range,
                "Hrs": "",
                "Lateral": "",
                "Phase": "",
                "Cat.": "",
                "Major OP": "",
                "Action": "",
                "Object": "",
                "Resp. Co": "",
                "Hole Depth Start": "",
                "Hole Depth End": "",
                "Event Depth Start": "",
                "Event Depth End": "",
                "Summary of Operations": summary_text
            })
        else:
            i += 1
    return rows

def parse_limited_morning_report(pdf_path: str):
    doc = fitz.open(pdf_path)
    text_pages = [doc.load_page(p).get_text() for p in range(len(doc))]
    full_text = "\n".join(text_pages)
    reports = re.split(r'(?=Limited Morning Report for \d{2}/\d{2}/\d{4})', full_text)
    reports = [r.strip() for r in reports if "Limited Morning Report for" in r]

    structured_data = []
    summary_operations_data = []

    for report in reports:
        row = {}
        date = extract_val(r'Limited Morning Report for\s+(\d{2}/\d{2}/\d{4})', report)
        rig = extract_val(r'Rig\s+(.*?)\n', report)
        well = extract_val(r'Well\s+(.*?)\n', report)

        row["Date"] = date
        row["Rig"] = rig
        row["Well Name"] = well
        row["Location"] = extract_val(r'Location\s+(.*?)\n', report)
        row["Objective"] = extract_val(r'Objective\s*:\s*\(?([^)]+)\)?', report)
        row["Thuraya"] = extract_val(r'THURAYA\s*\n?([+0-9 \-]+)', report)
        row["RIG FORMAN VSAT"] = extract_val(r'RIG FORMAN VSAT\s+([^\n]+)', report)
        row["Contractor VSAT"] = extract_val(r'CONTRACTOR\s+/CLERK VSA\s+T\s+([^\n]+)', report)
        row["Foreman(s)"] = "; ".join(re.findall(r'Foreman\(s\)(.*?)\n', report))
        row["Engineer(s)"] = "; ".join(re.findall(r'Engineer\s+(.*?)\n', report))
        row["Manager(s)"] = "; ".join(re.findall(r'Manager\s+(.*?)\n', report))
        row["Current Depth (ft)"] = extract_val(r'Current Depth \(ft\)\s+([0-9,]+)', report)
        row["Prev. Depth (ft)"] = extract_val(r'Prev. Depth \(ft\)\s+([0-9,]+)', report)
        row["Last Casing Size"] = extract_val(r'Last Csg Size\s+([^\n]+)', report)
        row["Liner Size"] = extract_val(r'Liner Size\s+([^\n]*)', report)
        row["Last 24 hr Operations"] = extract_val(r'Last 24 hr operations\s+(.*?)\n', report)
        row["Next 24 hr Plan"] = extract_val(r'Next 24 hr plan\s+(.*?)\n', report)

        # Other fields omitted for brevity — keep them as-is from your current code...
        # [KEEP ALL EXISTING FIELDS HERE]

        # Append structured data
        structured_data.append(row)

        # Extract summary of operations per report
        summary_ops = extract_summary_operations(report, date, rig, well)
        summary_operations_data.extend(summary_ops)

    return pd.DataFrame(structured_data), pd.DataFrame(summary_operations_data)

@app.route("/parse-batch", methods=["POST"])
def parse_batch():
    if 'files' not in request.files:
        return "No files uploaded", 400

    files = request.files.getlist("files")
    structured_combined = []
    summary_combined = []

    for file in files:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
            file.save(tmp.name)
            try:
                structured_df, summary_df = parse_limited_morning_report(tmp.name)
                structured_combined.append(structured_df)
                summary_combined.append(summary_df)
            finally:
                os.unlink(tmp.name)

    df_main = pd.concat(structured_combined, ignore_index=True)
    df_summary = pd.concat(summary_combined, ignore_index=True)

    # Escape formulas
    for df in [df_main, df_summary]:
        for col in df.select_dtypes(include='object').columns:
            df[col] = df[col].apply(lambda x: f"'{x}" if isinstance(x, str) and x.startswith('=') else x)

    output_path = "LMR_Final_With_Summary.xlsx"
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        df_main.to_excel(writer, sheet_name="Well Data", index=False)
        df_summary.to_excel(writer, sheet_name="Summary of Operations", index=False)

    return send_file(output_path, as_attachment=True, download_name="LMR_Final_With_Summary.xlsx")

@app.route("/")
def home():
    return "DMR Batch Parser is running."

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
