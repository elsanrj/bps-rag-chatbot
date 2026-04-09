"""
statictable_preprocessor.py
Fase 1B: Preprocessing Tabel Statis → Unified Document Schema

Tanggung jawab modul ini:
  - Baca raw_docs (source_type: statictable) dari MongoDB
  - Strip CSS dan noise dari HTML tabel (hasil export Excel dari BPS)
  - Klasifikasikan baris tabel: judul, header, data, nomor, footer, kosong
  - Parse struktur tabel: handle merged cells (colspan/rowspan)
  - Konversi baris data menjadi kalimat deskriptif dalam Bahasa Indonesia
  - Chunking: satu baris data = satu dokumen (konsisten dengan SIMDASI)
  - Simpan hasil ke MongoDB collection `documents` (unified schema)

Tantangan utama HTML tabel statis BPS:
  - HTML adalah hasil export Excel — penuh CSS inline dan merged cells
  - Merged cells (colspan/rowspan) di header harus di-expand dulu
    agar setiap kolom data bisa di-assign header yang benar
  - Ada tabel dengan multi-section (judul + header + data berulang) dalam satu HTML
  - Judul tabel selalu disertakan di awal setiap chunk agar tidak
    kehilangan konteks saat dipotong

Prasyarat:
  - statictable_ingestor.py sudah dijalankan (raw_docs terisi)
  - cleaner.py dan regions.py tersedia di direktori yang sama
  - pip install beautifulsoup4

Jalankan:
  python -m ingestion.statictable_preprocessor

Env variables yang diperlukan (.env):
  MONGODB_URI : Connection string MongoDB Atlas
  MONGODB_DB  : Nama database (contoh: bps_rag)
"""

import os
import logging

from datetime import datetime
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError
from bs4 import BeautifulSoup, Comment
from html import unescape

from ingestion.cleaner import (
    normalize_whitespace,
    apply_symbol_legend,
    validate_doc,
    generate_id,
)
from ingestion.regions import DISTRICTS, VILLAGES

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

MONGODB_URI  = os.getenv("MONGODB_URI")
MONGODB_DB   = os.getenv("MONGODB_DB", "bps_rag")
RAW_COLL     = "raw_docs"
DOC_COLL     = "documents"
BATCH_SIZE   = 100

# Kategori klasifikasi baris tabel
ROW_TITLE  = "title"    # baris judul tabel / sub-section (colspan besar)
ROW_HEADER = "header"   # baris label kolom
ROW_DATA   = "data"     # baris nilai statistik
ROW_NUMBER = "number"   # baris nomor kolom (1,2,3) — dibuang
ROW_FOOTER = "footer"   # baris catatan/sumber — dibuang
ROW_EMPTY  = "empty"    # baris kosong — dibuang

# ---------------------------------------------------------------------------
# Region resolver (konsisten dengan dynamic_preprocessor)
# ---------------------------------------------------------------------------

_REGION_KEYWORDS_JABAR = [
    "jawa barat", "jabar", "provinsi jawa barat",
]

def resolve_region(title: str) -> str:
    """
    Cari nama kecamatan atau kelurahan dalam title secara case-insensitive.
    Return nama wilayah jika ditemukan, default 'Kota Bandung' jika tidak.
    Prioritas: kecamatan lebih panjang dicek dulu untuk hindari partial match.
    """
    title_low = title.lower()
    # Strategi: Urutkan kunci berdasarkan panjang karakter (descending) 
    # agar nama panjang tidak terpotong (misal: "Bandung Kidul" vs "Bandung")
    sorted_villages = sorted(VILLAGES.items(), key=len, reverse=True)
    sorted_districts = sorted(DISTRICTS.items(), key=len, reverse=True)
    
    for name, display in sorted_villages:
        if name in title_low: return display

    for name, display in sorted_districts:
        if name in title_low: return display
        
    for keyword in _REGION_KEYWORDS_JABAR:
        if keyword in title_low:
            return "Provinsi Jawa Barat"
        
    return "Kota Bandung"

# ---------------------------------------------------------------------------
# HTML table parser
# ---------------------------------------------------------------------------

def strip_html_noise(html: str) -> BeautifulSoup:
    """
    Parse HTML tabel dan buang semua noise:
      - unescape HTML entities (misal &nbsp; → spasi)
      - Tag <style> dan <head> (CSS panjang dari Excel export)
      - Atribut class, style, width, height inline di semua tag
      - Tag <colgroup>, <col> (hanya untuk layout)
      - Komentar HTML
      - Trailing empty columns (artefak Excel — kolom cadangan kosong)
      
    Handle dua wrapper format:
      - Dengan <html>...</html>  (Item 0, 1, 3)
      - Langsung <table>...</table> tanpa wrapper (Item 2)

    Return BeautifulSoup object yang sudah bersih.
    """
    stripped = unescape(html)
    soup = BeautifulSoup(stripped, "html.parser")

    # Hapus tag yang tidak relevan
    for tag in soup.find_all(["style", "head", "colgroup", "col", "script"]):
        tag.decompose()

    # Hapus komentar HTML
    for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
        comment.extract()

    # Hapus atribut styling dari semua tag — tidak perlu untuk konversi teks
    for tag in soup.find_all(True):
        for attr in ["class", "style", "width", "height", "bgcolor",
                     "align", "valign", "border", "cellpadding",
                     "cellspacing", "x:publishsource"]:
            tag.attrs.pop(attr, None)
            
    # Buang trailing empty columns (artefak Excel — kolom cadangan kosong)
    # Berlaku untuk Item 3 yang punya 6 trailing empty columns
    table = soup.find("table")
    if table:
        rows = table.find_all("tr")
        if rows:
            # Hitung max kolom
            max_cols = max(
                sum(int(c.get("colspan", 1)) for c in row.find_all(["td", "th"]))
                for row in rows
            )
            # Cek dari kanan: kolom mana yang semua baris kosong
            trailing_empty = 0
            for col_idx in range(max_cols - 1, -1, -1):
                col_vals = []
                for row in rows:
                    cells = row.find_all(["td", "th"])
                    if col_idx < len(cells):
                        col_vals.append(cells[col_idx].get_text(strip=True))
                if not any(col_vals):
                    trailing_empty += 1
                else:
                    break

            # Hapus trailing empty columns dari setiap baris
            if trailing_empty > 0:
                for row in rows:
                    cells = row.find_all(["td", "th"])
                    for cell in cells[len(cells) - trailing_empty:]:
                        cell.decompose()

    return soup

def classify_rows(rows: list) -> list[tuple[str, any]]:
    """
    Klasifikasikan setiap baris tabel ke salah satu dari 6 kategori:
      ROW_EMPTY  : semua cell kosong → dibuang
      ROW_NUMBER : semua cell berisi angka/nomor urut (1),(2),(3) → dibuang
      ROW_FOOTER : baris catatan atau sumber di bagian bawah → dibuang
      ROW_TITLE  : satu cell dengan colspan besar = judul tabel/section
      ROW_HEADER : label kolom (mayoritas <th> atau teks non-angka)
      ROW_DATA   : baris nilai statistik

    Return list of (kategori, row_element).
    """
    classified = []
    found_data  = False   # flag: sudah lewat baris data pertama

    for row in rows:
        cells      = row.find_all(["td", "th"])
        cell_texts = [c.get_text(strip=True) for c in cells]
        non_empty  = [t for t in cell_texts if t]

        # --- ROW_EMPTY ---
        if not non_empty:
            classified.append((ROW_EMPTY, row))
            continue

        # --- ROW_NUMBER ---
        # Baris nomor kolom: semua cell adalah angka atau (angka)
        def is_col_number(text):
            t = text.strip().replace("(", "").replace(")", "")
            return t.isdigit()

        if non_empty and all(is_col_number(t) for t in non_empty):
            classified.append((ROW_NUMBER, row))
            continue

        # --- ROW_FOOTER ---
        # Baris catatan/sumber: cell pertama diawali kata kunci tertentu
        first_text = non_empty[0].lower() if non_empty else ""
        footer_keywords = ["catatan", "sumber", "source", "note", "keterangan", "ket."]
        if any(first_text.startswith(kw) for kw in footer_keywords):
            classified.append((ROW_FOOTER, row))
            found_data = True   # footer selalu setelah data
            continue

        # --- ROW_TITLE ---
        # Satu cell dengan colspan >= 3 yang berisi teks panjang (bukan angka)
        if len(non_empty) == 1:
            cs = int(cells[0].get("colspan", 1)) if cells else 1
            is_numeric = non_empty[0].replace(".", "").replace(",", "").isdigit()
            if cs >= 3 and not is_numeric:
                classified.append((ROW_TITLE, row))
                continue

        # --- ROW_HEADER vs ROW_DATA ---
        # Setelah ditemukan data, baris TITLE baru = section baru
        # Heuristik header: mayoritas cell <th>, atau semua cell non-angka
        th_count  = len(row.find_all("th"))
        td_count  = len(row.find_all("td"))
        total     = th_count + td_count

        def is_numeric_cell(text):
            t = text.replace(".", "").replace(",", "").replace("-", "").replace("%", "").strip()
            return t.isdigit() if t else False

        numeric_ratio = sum(1 for t in non_empty if is_numeric_cell(t)) / len(non_empty)

        is_header = (
            (th_count > 0 and th_count >= td_count) or
            numeric_ratio < 0.3   # < 30% cell berisi angka → header
        )

        if is_header and not found_data:
            classified.append((ROW_HEADER, row))
        else:
            classified.append((ROW_DATA, row))
            found_data = True

    return classified

def split_into_sections(classified_rows: list[tuple[str, any]]) -> list[dict]:
    """
    Pisahkan classified_rows menjadi list section independen.

    Satu section = satu kelompok {title, header_rows, data_rows}.
    Multi-section terdeteksi saat ada pola TITLE → HEADER → DATA yang berulang
    (contoh: Item 1 yang berisi 3 sub-tabel dalam 1 HTML).

    Section tanpa title menggunakan string kosong sebagai title —
    judul tabel utama akan ditambahkan di tahap konversi kalimat.

    Return list of dict:
      [
        {
          "section_title" : str,   # judul sub-section, "" jika tidak ada
          "header_rows"   : list,  # list of row elements untuk expand_merged_headers
          "data_rows"     : list,  # list of row elements untuk konversi kalimat
        },
        ...
      ]
    """
    sections     = []
    current      = {"section_title": "", "header_rows": [], "data_rows": []}
    has_data     = False

    for category, row in classified_rows:
        if category in (ROW_EMPTY, ROW_NUMBER, ROW_FOOTER):
            continue

        if category == ROW_TITLE:
            title_text = normalize_whitespace(row.get_text(separator=" "))

            if has_data:
                # Simpan section sebelumnya, mulai section baru
                if current["data_rows"]:
                    sections.append(current)
                current  = {"section_title": title_text, "header_rows": [], "data_rows": []}
                has_data = False
            else:
                # Masih di awal — ini judul section pertama
                current["section_title"] = title_text

        elif category == ROW_HEADER:
            current["header_rows"].append(row)

        elif category == ROW_DATA:
            current["data_rows"].append(row)
            has_data = True

    # Simpan section terakhir
    if current["data_rows"]:
        sections.append(current)

    return sections

def expand_merged_headers(header_rows: list) -> list[list[str]]:
    """
    Expand merged cells (colspan/rowspan) di baris header menjadi
    grid header yang lengkap — setiap kolom data punya label yang benar.

    Tantangan: Excel export BPS sering menggunakan multi-row header
    dengan colspan dan rowspan untuk mengelompokkan kolom.

    Strategi:
      - Bangun grid 2D menggunakan dict {(row, col): text}
      - Untuk setiap cell dengan colspan/rowspan, isi semua posisi
        yang dicakup dengan teks yang sama
      - Gabungkan header multi-row per kolom dengan separator " - "

    Return list of str: satu label per kolom data.
    """
    grid = {}   # {(row_idx, col_idx): text}

    for row_idx, row in enumerate(header_rows):
        col_idx = 0
        for cell in row.find_all(["td", "th"]):
            # Skip posisi yang sudah diisi oleh rowspan sebelumnya
            while (row_idx, col_idx) in grid:
                col_idx += 1

            text = normalize_whitespace(cell.get_text(separator=" "))

            try:
                colspan = int(cell.get("colspan", 1))
            except (ValueError, TypeError):
                colspan = 1
            try:
                rowspan = int(cell.get("rowspan", 1))
            except (ValueError, TypeError):
                rowspan = 1

            # Isi semua posisi yang dicakup cell ini
            for r in range(row_idx, row_idx + rowspan):
                for c in range(col_idx, col_idx + colspan):
                    if (r, c) not in grid:
                        grid[(r, c)] = text

            col_idx += colspan

    if not grid:
        return []

    max_row = max(r for r, c in grid) + 1
    max_col = max(c for r, c in grid) + 1

    # Gabungkan label multi-row per kolom
    headers = []
    for col_idx in range(max_col):
        parts = []
        for row_idx in range(max_row):
            text = grid.get((row_idx, col_idx), "")
            if text and text not in parts:
                parts.append(text)
        headers.append(" - ".join(parts) if parts else f"Kolom {col_idx + 1}")

    return headers


def parse_table_html(html: str) -> list[dict]:
    """
    Parse HTML tabel BPS menjadi list section siap konversi.

    Pipeline:
      [1] strip_html_noise  — unescape, buang CSS/noise, trim trailing cols
      [2] classify_rows     — kategorikan setiap baris
      [3] split_into_sections — pisahkan multi-section
      [4] expand_merged_headers — per section

    Return list of dict (section):
      [
        {
          "section_title" : str,
          "headers"       : list[str],
          "data_rows"     : list[list[str]],
        },
        ...
      ]
    Return list kosong jika tidak ada data yang bisa diekstrak.
    """
    soup  = strip_html_noise(html)
    table = soup.find("table")

    if not table:
        logger.debug("Tidak ada tag <table> ditemukan dalam HTML")
        return []

    all_rows = table.find_all("tr")
    if not all_rows:
        return []
    
    # [2] Klasifikasi
    classified = classify_rows(all_rows)

    # [3] Pisahkan section
    sections = split_into_sections(classified)

    if not sections:
        return []

    # [4] Expand headers + extract nilai per section
    result = []
    for section in sections:
        headers   = expand_merged_headers(section["header_rows"])
        data_rows = []

        for row in section["data_rows"]:
            cells = row.find_all(["td", "th"])
            if not cells:
                continue
            row_values = []
            for cell in cells:
                text = normalize_whitespace(cell.get_text(separator=" "))
                text = apply_symbol_legend(text)
                row_values.append(text)
            if any(v.strip() for v in row_values):
                data_rows.append(row_values)

        if data_rows:
            result.append({
                "section_title" : section["section_title"],
                "headers"       : headers,
                "data_rows"     : data_rows,
            })
    
    return result


# ---------------------------------------------------------------------------
# Konversi baris ke kalimat
# ---------------------------------------------------------------------------

def rows_to_sentences(
    title: str,
    headers: list[str],
    rows: list[list[str]],
    section_title: str = "",
) -> str:
    """
    Konversi list baris data tabel menjadi paragraf kalimat deskriptif.

    Template per baris:
      {cell[0]}: {header[1]} sebesar {cell[1]}, {header[2]} sebesar {cell[2]}, ...

    Asumsi: kolom pertama (index 0) adalah label baris (nama wilayah/kategori).
    Kolom berikutnya adalah nilai data dengan header sebagai label.
    
    section_title disertakan jika ada (multi-section) sebagai konteks tambahan.

    Contoh output:
      Tabel: Jumlah RW dan RT di Kecamatan Cibeunying Kidul, 2018-2023.
      Kelurahan Padasuka: RW sebesar 12, RT sebesar 67.
      Kelurahan Sukapada: RW sebesar 8, RT sebesar 45.
    """
    if not rows:
        return ""

    # Header: judul tabel + section jika ada
    if section_title and section_title.lower() != title.lower():
        sentences = [f"Tabel: {title}. {section_title}."]
    else:
        sentences = [f"Tabel: {title}."]

    for row in rows:
        if not row:
            continue

        # Kolom pertama sebagai label baris
        row_label = row[0].strip() if row else ""
        if not row_label:
            continue

        # Pasangkan nilai dengan header
        value_parts = []
        for i, value in enumerate(row[1:], start=1):
            value = value.strip()
            if not value or value.lower() in ("data tidak tersedia", "tidak dapat ditampilkan"):
                continue

            header = headers[i] if i < len(headers) else f"Kolom {i + 1}"
            value_parts.append(f"{header} sebesar {value}")

        if value_parts:
            sentence = f"{row_label}: {', '.join(value_parts)}."
            sentences.append(sentence)

    return normalize_whitespace(" ".join(sentences))


# ---------------------------------------------------------------------------
# Chunking tabel besar
# ---------------------------------------------------------------------------

def rows_to_documents(
    title: str,
    headers: list[str],
    data_rows: list[list[str]],
    section_title: str = "",
) -> list[str]:
    """
    Konversi setiap baris data menjadi satu dokumen mandiri.
    1 baris = 1 dokumen — konsisten dengan SIMDASI dan Dynamic Data.

    Setiap dokumen membawa konteks penuh:
      judul tabel + section title (jika ada) + header kolom + nilai baris.

    Contoh output per dokumen:
      "Tabel: Jumlah Pelayanan Kependudukan di Kecamatan Panyileukan, 2023.
       Kelurahan Mekar Mulya: KK sebesar 460, Serba Guna sebesar 274,
       Lainnya sebesar 281."
    """
    documents = []

    for row in data_rows:
        content = rows_to_sentences(title, headers, [row], section_title)
        if content:
            documents.append(content)

    return documents

# ---------------------------------------------------------------------------
# Proses satu raw tabel statis
# ---------------------------------------------------------------------------

def process_one_table(raw_doc: dict) -> list[dict]:
    """
    Proses satu raw tabel statis dari MongoDB menjadi list unified documents.
    Satu tabel bisa menghasilkan beberapa dokumen.

    Parameter:
      raw_doc : Satu dokumen dari collection raw_docs (source_type: statictable)

    Return list of unified document dicts (siap upsert ke collection `documents`).
    """
    table_id   = raw_doc.get("table_id")
    source_url = raw_doc.get("source_url")

    # Verifikasi struktur payload sebelum akses field
    payload = raw_doc.get("payload", {})

    if not payload:
        logger.warning(f"Payload kosong untuk table_id={table_id}")
        return []

    # Akses field dari payload — verifikasi keys dulu sebelum pakai
    # (pelajaran dari dynamic data: jangan asumsikan struktur)
    title    = payload.get("title", raw_doc.get("title", ""))
    table_html = payload.get("table", "")
    subcsa   = payload.get("subcsa", "")
    cr_date  = payload.get("cr_date", "")
    updt_date = payload.get("updt_date", "")
    related  = payload.get("related", [])

    if not title:
        logger.warning(f"Title kosong untuk table_id={table_id}, skip.")
        return []

    if not table_html or not table_html.strip():
        logger.warning(f"Field 'table' kosong untuk table_id={table_id}, skip.")
        return []

    # Parse HTML tabel
    sections = parse_table_html(table_html)

    if not sections:
        logger.debug(f"Tidak ada section data untuk table_id={table_id}")
        return []

    # Kumpulkan semua chunks dari semua section
    all_docs = []
    for section in sections:
        section_docs = rows_to_documents(
            title         = title,
            headers       = section["headers"],
            data_rows     = section["data_rows"],
            section_title = section["section_title"],
        )
        all_docs.extend(section_docs)

    content_chunks = all_docs

    if not content_chunks:
        logger.debug(f"Tidak ada content yang dihasilkan untuk table_id={table_id}")
        return []

    # Tentukan tanggal — prioritaskan updt_date, fallback cr_date
    date     = updt_date or cr_date or None
    region   = resolve_region(title)

    # Simpan related sebagai list id untuk cross-reference
    related_ids = [
        r.get("id") for r in related
        if isinstance(r, dict) and r.get("id")
    ]

    unified_docs = []

    for chunk_idx, content in enumerate(content_chunks):
        candidate = {"content": content, "id": generate_id(content)}
        if not validate_doc(candidate):
            continue

        unified_doc = {
            "id"          : candidate["id"],
            "source_type" : "statictable",
            "title"       : title,
            "content"     : content,
            "metadata"    : {
                "date"       : date,
                "year"       : int(date[:4]) if date and date[:4].isdigit() else None,
                "category"   : subcsa,
                "region"     : region,
                "source_url" : source_url,
                "extra"      : {
                    "table_id"    : table_id,
                    "row_index" : chunk_idx,              # urutan baris dalam tabel
                    "total_rows": len(content_chunks),   # total dokumen tabel ini
                    "total_sections": len(sections),
                    "related_ids" : related_ids,           # cross-reference tabel terkait
                },
            },
            "processed_at" : datetime.utcnow(),
        }

        unified_docs.append(unified_doc)

    return unified_docs


# ---------------------------------------------------------------------------
# Simpan ke collection documents
# ---------------------------------------------------------------------------

def save_documents(docs: list[dict], collection) -> int:
    """
    Upsert list unified documents ke MongoDB collection `documents`.
    Upsert berdasarkan field `id` (MD5 hash dari content) — idempotent.
    Return jumlah dokumen yang berhasil disimpan.
    """
    if not docs:
        return 0

    operations = [
        UpdateOne(
            filter={"id": doc["id"]},
            update={"$set": doc},
            upsert=True,
        )
        for doc in docs
    ]

    try:
        result = collection.bulk_write(operations, ordered=False)
        return result.upserted_count + result.modified_count
    except BulkWriteError as e:
        logger.error(f"BulkWriteError: {e.details}")
        return 0


# ---------------------------------------------------------------------------
# Orchestrator utama
# ---------------------------------------------------------------------------

def run():
    """
    Pipeline utama preprocessing tabel statis:
      1. Iterasi semua raw_docs (source_type: statictable)
      2. Parse HTML → extract headers + data rows
      3. Chunk baris → konversi ke kalimat → unified documents
      4. Upsert ke collection `documents`
    """
    if not MONGODB_URI:
        logger.error("MONGODB_URI tidak ditemukan di environment. Hentikan.")
        return

    client   = MongoClient(MONGODB_URI)
    db       = client[MONGODB_DB]
    raw_coll = db[RAW_COLL]
    doc_coll = db[DOC_COLL]

    logger.info(f"Terhubung ke MongoDB: {MONGODB_DB}")

    total_raw    = raw_coll.count_documents({"source_type": "statictable"})
    total_saved  = 0
    total_skip   = 0
    total_chunks = 0
    batch        = []

    logger.info(f"Total raw tabel statis yang akan diproses: {total_raw}")

    for i, raw_doc in enumerate(raw_coll.find({"source_type": "statictable"}), start=1):
        table_id = raw_doc.get("table_id", "unknown")

        try:
            unified_docs = process_one_table(raw_doc)
        except Exception as e:
            # exc_info=True untuk full traceback — penting saat debugging
            logger.error(
                f"Gagal memproses table_id={table_id}. Error: {e}",
                exc_info=True,
            )
            total_skip += 1
            continue

        if not unified_docs:
            logger.debug(f"[{i}/{total_raw}] Skip: table_id={table_id}")
            total_skip += 1
            continue

        total_chunks += len(unified_docs)

        # Append per dokumen — jaga batch size tetap terkontrol
        for doc in unified_docs:
            batch.append(doc)
            if len(batch) >= BATCH_SIZE:
                saved = save_documents(batch, doc_coll)
                total_saved += saved
                logger.info(f"  [{i}/{total_raw}] Batch flushed: {saved} dokumen disimpan")
                batch = []

        logger.info(
            f"[{i}/{total_raw}] OK: table_id={table_id} "
            f"→ {len(unified_docs)} chunk(s)"
        )

    # Flush sisa batch
    if batch:
        saved = save_documents(batch, doc_coll)
        total_saved += saved
        logger.info(f"Final batch flushed: {saved} dokumen disimpan")

    # --- Summary ---
    logger.info("=" * 50)
    logger.info("PREPROCESSING TABEL STATIS SELESAI")
    logger.info(f"  Total raw diproses : {total_raw}")
    logger.info(f"  Total chunk dibuat : {total_chunks}")
    logger.info(f"  Dokumen disimpan   : {total_saved}")
    logger.info(f"  Dilewati           : {total_skip}")
    logger.info("=" * 50)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    run()
