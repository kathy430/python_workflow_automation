import pandas as pd
import pdfplumber
from pathlib import Path
from pdf2image import convert_from_path
import pytesseract
from rapidfuzz import fuzz

fda_prop65 = [
    "Cocamide DEA",
    "Cocamide MEA",
    "DEA-Cetyl Phosphate",
    "DEA Oleth-3 Phosphate",
    "Lauramide DEA",
    "Linoleamide MEA",
    "Myristamide DEA",
    "Oleamide DEA",
    "Stearamide MEA",
    "TEA-Lauryl Sulfate",
    "Triethanolamine"
]


target_folders = [
    Path(r"C:\Users\user\Documents\sample labels")
]

prop65_labels = []

def has_fuzzy_match(text, keywords, threshold=80):
    text = text.lower()
    
    for keyword in keywords:
        if fuzz.partial_ratio(keyword.lower(), text) >= threshold:
            return True, keyword
    
    return False, None

for folder in target_folders:
    for pdf_file in folder.glob("*.pdf"): 
        print(f"Processing: {pdf_file}")
        pdf_name = str(pdf_file).split("\\")[-1]
        
        try:
            with pdfplumber.open(pdf_file) as pdf:
                if not pdf.pages:
                    images = convert_from_path(
                        pdf_file,

                        poppler_path = r"C:\poppler-25.12.0\Library\bin",
                        dpi=400
                    )
                    
                    for img in images:
                        ocr_text = pytesseract.image_to_string(img)
                        
                        flag, matched_word = has_fuzzy_match(ocr_text, fda_prop65)
                        row_data = {
                            "file": pdf_name,
                            "file location": pdf_file,
                            "text": ocr_text,
                            "match": flag,
                            "matched word": matched_word
                        }

                        prop65_labels.append(row_data)
                else:
                    for page_num, page in enumerate(pdf.pages):
                        # try normal text extraction
                        text = page.extract_text(use_text_flow=True)
                        
                        if (len(text) == 4):
                            images = convert_from_path(
                            pdf_file,
                            first_page = page_num + 1,
                            last_page = page_num + 1,
                            poppler_path = r"C:\poppler-25.12.0\Library\bin",
                            dpi=400
                            )

                            for img in images:
                                ocr_text = pytesseract.image_to_string(img)
                                flag, matched_word = has_fuzzy_match(ocr_text, fda_prop65)
                                row_data = {
                                    "file": pdf_name,
                                    "file location": pdf_file,
                                    "text": ocr_text,
                                    "match": flag,
                                    "matched word": matched_word
                                }
                                prop65_labels.append(row_data)
                        elif text and text.strip():
                            flag, matched_word = has_fuzzy_match(text, fda_prop65)
                            row_data = {
                                "file": pdf_name,
                                "file location": pdf_file,
                                "text": text,
                                "match": flag,
                                "matched word": matched_word
                            }
                            prop65_labels.append(row_data)
                        # ocr fallback
                        else:
                            images = convert_from_path(
                            pdf_file,
                            first_page = page_num + 1,
                            last_page = page_num + 1,
                            poppler_path = r"C:\poppler-25.12.0\Library\bin",
                            dpi=400
                            )

                            for img in images:
                                ocr_text = pytesseract.image_to_string(img)
                                flag, matched_word = has_fuzzy_match(ocr_text, fda_prop65)
                                row_data = {
                                    "file": pdf_name,
                                    "file location": pdf_file,
                                    "text": ocr_text,
                                    "match": flag,
                                    "matched word": matched_word
                                }
                                prop65_labels.append(row_data)
                        
        except Exception as e:
            print(f"This file: {pdf_file} has an error: {e}")
            continue

df = pd.DataFrame(prop65_labels)

df.to_excel(r"C:\Users\user\Documents\test_label data.xlsx", index=False)