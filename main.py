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

def extract_summary_operations(report_text, date, rig, well):
    rows = []
    summary_blocks = re.findall(
        r'(\d{4}\s*-\s*\d{4}).*?(?=\n\d{4}\s*-\s*\d{4}|$)',
        report_text, re.DOTALL
    )
    for block in summary_blocks:
        from_to_match = re.search(r'(\d{4})\s*-\s*(\d{4})', block)
        from_to = f"{from_to_match.group(1)} - {from_to_match.group(2)}" if from_to_match else ""
        hrs = ""  # You can refine this if Hrs value appears
        lateral = extract_val(r'Lateral\s*:\s*([^\n]+)', block, "")
        phase = extract_val(r'Phase\s*:\s*([^\n]+)', block, "")
        cat = extract_val(r'Cat\.?\s*:\s*([^\n]+)', block, "")
        major_op = extract_val(r'Major OP\s*:\s*([^\n]+)', block, "")
        action = extract_val(r'Action\s*:\s*([^\n]+)', block, "")
        obj = extract_val(r'Object\s*:\s*([^\n]+)', block, "")
        resp = extract_val(r'Resp\. Co\s*:\s*([^\n]+)', block, "")
        hd_start = extract_val(r'Hole depth.*?Start\s*:? ([0-9]+)', block, "")
        hd_end = extract_val(r'Hole depth.*?End\s*:? ([0-9]+)', block, "")
        ed_start = extract_val(r'Event depth.*?Start\s*:? ([0-9]+)', block, "")
        ed_end = extract_val(r'Event depth.*?End\s*:? ([0-9]+)', block, "")
        summary = extract_val(r'(?:Summary of Operations|(?:\*\*|—|-)[^\n]*)([\s\S]+)', block, "").strip()
        rows.append({
            "Date": date,
            "Rig": rig,
            "Well": well,
            "From - To": from_to,
            "Hrs": hrs,
            "Lateral": lateral,
            "Phase": phase,
            "Cat.": cat,
            "Major OP": major_op,
            "Action": action,
            "Object": obj,
            "Resp. Co": resp,
            "Hole Depth Start": hd_start,
            "Hole Depth End": hd_end,
            "Event Depth Start": ed_start,
            "Event Depth End": ed_end,
            "Summary of Operations": summary
        })
    return rows

def parse_limited_morning_report(pdf_path: str):
    doc = fitz.open(pdf_path)
    text_pages = [doc.load_page(p).get_text() for p in range(len(doc))]
    full_text = "\n".join(text_pages)
    reports = re.split(r'(?=Limited Morning Report for \d{2}/\d{2}/\d{4})', full_text)
    reports = [r.strip() for r in reports if "Limited Morning Report for" in r]

    well_data = []
    ops_data = []

    for report in reports:
        row = {}
        row["Well Name"] = extract_val(r'Well\s+(.*?)\n', report)
        row["Rig"] = extract_val(r'Rig\s+(.*?)\n', report)
        row["Date"] = extract_val(r'Limited Morning Report for\s+(\d{2}/\d{2}/\d{4})', report)
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

        muds = re.findall(r'Weight\s+([0-9.]+)\s+PCF.*?Funnel\s+Vis\.\(SEC\)\s+([0-9.]+).*?PV\s+([0-9.]+).*?YP\s+([0-9.]+)', report, re.DOTALL)
        for i, mud in enumerate(muds[:3]):
            row[f"Mud {i+1} Weight (PCF)"] = mud[0]
            row[f"Mud {i+1} Funnel Vis (sec)"] = mud[1]
            row[f"Mud {i+1} PV"] = mud[2]
            row[f"Mud {i+1} YP"] = mud[3]

        tops = re.findall(r'([A-Z]{2,10})\s+([0-9,]+)\s+([^\n]*)', report)
        for i, top in enumerate(tops[:5]):
            row[f"Formation {i+1} Name"] = top[0]
            row[f"Formation {i+1} Depth"] = top[1]
            row[f"Formation {i+1} Comment"] = top[2]

        personnel = re.findall(r'([A-Z0-9]{2,5})\s+([A-Z]{3,5})\s+([0-9]{1,3})', report)
        for i, person in enumerate(personnel[:5]):
            row[f"Personnel {i+1} Company"] = person[0]
            row[f"Personnel {i+1} Category"] = person[1]
            row[f"Personnel {i+1} Count"] = person[2]

        row["GOR"] = extract_val(r'GOR\s*[:=]?\s*([0-9,]+)\s*SCF/BBL', report)
        row["H2S"] = extract_val(r'H2S\s*[:=]?\s*([0-9.]+)\s*%', report)
        rer_texts = re.findall(r'RER[^:\n]*[:=]?\s*[^.\n]*?(?:PPM|LFL)[^\n.]*', report, re.IGNORECASE)
        row["RER Readings"] = " | ".join(r.strip() for r in rer_texts if "PPM" in r or "LFL" in r)

        row["Foreman Remarks"] = extract_val(r'Foreman Remarks\s*\n(.*?)(?:Page \d+ of \d+|\n\s*Saudi Aramco|mailto:)', report, "")
        row["Operations Timeline"] = " | ".join([re.sub(r'\s+', ' ', t.strip()) for t in re.findall(r'\d{4} - \d{4}.*?Summary of Operations(.*?)\n(?=\d{4} - \d{4}|\n\s*\*|Foreman Remarks)', report, re.DOTALL)])

        drill_string = re.findall(r'Order Component Provider.*?Serial \n#\n(.*?)\n\n', report, re.DOTALL)
        row["Drill String"] = None if drill_string and "Page" in drill_string[0] else drill_string[0].strip() if drill_string else None

        survey = re.search(r'DAILY SURVEY\s*\n(.*?)\n\n', report, re.DOTALL)
        row["Directional Survey"] = None if survey and "Page" in survey.group(1) else survey.group(1).strip() if survey else None

        row["Wind"] = extract_val(r'Wind\s+([A-Z]+)', report)
        row["Sea"] = extract_val(r'Sea\s+([A-Z]+)', report)
        row["Weather"] = extract_val(r'Weather\s+([A-Z]+)', report)

        svc = re.search(r'SERVICE COMPANY, RENTAL TOOLS & OTHERS\s*={5,}\s*(.*?)\s*={5,}', report, re.DOTALL | re.IGNORECASE)
        row["Service Tools"] = re.sub(r'\s+', ' ', svc.group(1).strip()) if svc else None

        well_data.append(row)
        ops_data.extend(extract_summary_operations(report, row["Date"], row["Rig"], row["Well Name"]))

    return pd.DataFrame(well_data), pd.DataFrame(ops_data)

@app.route("/parse-batch", methods=["POST"])
def parse_batch():
    if 'files' not in request.files:
        return "No files uploaded", 400

    files = request.files.getlist("files")
    well_data = []
    summary_ops = []

    for file in files:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
            file.save(tmp.name)
            try:
                wd_df, so_df = parse_limited_morning_report(tmp.name)
                well_data.append(wd_df)
                summary_ops.append(so_df)
            finally:
                os.unlink(tmp.name)

    final_wd = pd.concat(well_data, ignore_index=True)
    final_so = pd.concat(summary_ops, ignore_index=True)

    for col in final_wd.select_dtypes(include='object').columns:
        final_wd[col] = final_wd[col].apply(lambda x: f"'{x}" if isinstance(x, str) and x.startswith('=') else x)

    output_path = "combined_output.xlsx"
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        final_wd.to_excel(writer, index=False, sheet_name="Well Data")
        final_so.to_excel(writer, index=False, sheet_name="Summary of Operations")

    return send_file(output_path, as_attachment=True, download_name="LMR_Well_Data.xlsx")

@app.route("/")
def home():
    return "DMR Batch Parser is running."

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
