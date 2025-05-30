from flask import Flask, request, send_file
from flask_cors import CORS
import fitz  # PyMuPDF
import re
import pandas as pd
import tempfile
import os
from io import BytesIO
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows

app = Flask(__name__)
CORS(app)

def extract_val(pattern, text, default=None):
    match = re.search(pattern, text)
    return match.group(1).strip() if match else default

def extract_summary_table(report, date, rig, well):
    rows = []
    summary_tables = re.findall(
        r'(\d{4}\s*-\s*\d{4})\s+([0-9.]+)\s+([^\s]+)\s+([^\s]+)\s+([^\s]+)\s+([^\s]+)\s+([^\s]+)\s+([^\s]+)\s+([^\s]+)\s+([^\s]*)\s+([^\s]*)\s+(.*?)(?=\n\d{4}\s*-\s*\d{4}|\Z)',
        report,
        re.DOTALL
    )
    for row in summary_tables:
        rows.append({
            "Date": date,
            "Rig": rig,
            "Well": well,
            "From - To": row[0],
            "Hrs": row[1],
            "Lateral": row[2],
            "Phase": row[3],
            "Cat.": row[4],
            "Major OP": row[5],
            "Action": row[6],
            "Object": row[7],
            "Resp. Co": row[8],
            "Hole Depth Start": row[9],
            "Hole Depth End": row[10],
            "Summary of Operations": re.sub(r'\s+', ' ', row[11]).strip()
        })
    return rows

def parse_limited_morning_report(pdf_path: str):
    doc = fitz.open(pdf_path)
    text_pages = [doc.load_page(p).get_text() for p in range(len(doc))]
    full_text = "\n".join(text_pages)
    reports = re.split(r'(?=Limited Morning Report for \d{2}/\d{2}/\d{4})', full_text)
    reports = [r.strip() for r in reports if "Limited Morning Report for" in r]

    well_data = []
    summary_data = []

    for report in reports:
        row = {}
        well = extract_val(r'Well\s+(.*?)\n', report)
        rig = extract_val(r'Rig\s+(.*?)\n', report)
        date = extract_val(r'Limited Morning Report for\s+(\d{2}/\d{2}/\d{4})', report)

        row["Well Name"] = well
        row["Rig"] = rig
        row["Date"] = date
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
        row["DSLTA"] = extract_val(r'DSLTA\s+([0-9,]+)', report)
        row["Safety Meeting"] = extract_val(r'Safety Meeting\s+([^\n]+)', report)
        row["JSA_Count"] = extract_val(r'JSA:\s*\(?([0-9]+)', report)
        row["PTW_Count"] = extract_val(r'PTW:\s*\(?([0-9]+)', report)
        row["Stop Cards"] = extract_val(r'STOP CARDS:\s*\(?([0-9]+)', report)
        row["Near Miss"] = extract_val(r'NEAR MISS:\s*\(?([0-9]+)', report)
        row["Bit Number"] = extract_val(r'Bit Number\s+([^\n]+)', report)
        row["Bit Size"] = extract_val(r'Size\s+([^\n]+)', report)
        row["WOB"] = extract_val(r'WOB\s+([^\n]+)', report)
        row["RPM"] = extract_val(r'RPM\s+([^\n]+)', report)

        # Mud
        muds = re.findall(r'Weight\s+([0-9.]+)\s+PCF.*?Funnel\s+Vis\.\(SEC\)\s+([0-9.]+).*?PV\s+([0-9.]+).*?YP\s+([0-9.]+)', report, re.DOTALL)
        for i, mud in enumerate(muds[:3]):
            row[f"Mud {i+1} Weight (PCF)"] = mud[0]
            row[f"Mud {i+1} Funnel Vis (sec)"] = mud[1]
            row[f"Mud {i+1} PV"] = mud[2]
            row[f"Mud {i+1} YP"] = mud[3]

        # Tops
        tops = re.findall(r'([A-Z]{2,10})\s+([0-9,]+)\s+([^\n]*)', report)
        for i, top in enumerate(tops[:5]):
            row[f"Formation {i+1} Name"] = top[0]
            row[f"Formation {i+1} Depth"] = top[1]
            row[f"Formation {i+1} Comment"] = top[2]

        well_data.append(row)
        summary_data.extend(extract_summary_table(report, date, rig, well))

    return pd.DataFrame(well_data), pd.DataFrame(summary_data)

@app.route("/parse-batch", methods=["POST"])
def parse_batch():
    if 'files' not in request.files:
        return "No files uploaded", 400

    files = request.files.getlist("files")
    all_well_data = []
    all_summary_data = []

    for file in files:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
            file.save(tmp.name)
            try:
                well_df, summary_df = parse_limited_morning_report(tmp.name)
                all_well_data.append(well_df)
                all_summary_data.append(summary_df)
            finally:
                os.unlink(tmp.name)

    well_df_final = pd.concat(all_well_data, ignore_index=True)
    summary_df_final = pd.concat(all_summary_data, ignore_index=True)

    for df in [well_df_final, summary_df_final]:
        for col in df.select_dtypes(include='object').columns:
            df[col] = df[col].apply(lambda x: f"'{x}" if isinstance(x, str) and x.startswith('=') else x)

    output_path = "LMR_Parsed_Final.xlsx"
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        well_df_final.to_excel(writer, index=False, sheet_name="Well Data")
        summary_df_final.to_excel(writer, index=False, sheet_name="Summary of Operations")

    return send_file(output_path, as_attachment=True, download_name="LMR_Parsed_Final.xlsx")

@app.route("/")
def home():
    return "DMR Batch Parser is running."

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)

from flask import jsonify

AUTHORIZED_PASSWORDS = {"oilfield2025", "clientXsecure", "aramco123"}

@app.route("/auth", methods=["POST"])
def auth():
    data = request.get_json()
    password = data.get("password", "")
    return jsonify({"success": password in AUTHORIZED_PASSWORDS})

