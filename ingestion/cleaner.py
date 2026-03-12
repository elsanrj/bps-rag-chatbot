"""
cleaner.py
Modul reusable untuk text cleaning — dipakai oleh semua ingestor/preprocessor.

Fungsi-fungsi di sini murni (pure functions): input teks → output teks bersih.
Tidak ada side effect, tidak ada koneksi ke database atau API.
"""

import re
import hashlib

from html.parser import HTMLParser


# ---------------------------------------------------------------------------
# HTML Cleaning
# ---------------------------------------------------------------------------

class _HTMLStripper(HTMLParser):
    """Parser internal untuk strip tag HTML dan decode entities."""

    def __init__(self):
        super().__init__()
        self.reset()
        self.fed = []

    def handle_data(self, d):
        self.fed.append(d)

    def get_data(self):
        return " ".join(self.fed)


def clean_html(text: str) -> str:
    """
    Strip semua tag HTML dari teks dan decode HTML entities.
    Contoh: '<p>Bandung &amp; sekitarnya</p>' → 'Bandung & sekitarnya'

    Berlaku untuk: BRS abstract, judul SIMDASI, field yang mungkin berisi HTML.
    """
    if not text or not isinstance(text, str):
        return ""

    stripper = _HTMLStripper()
    stripper.feed(text)
    return stripper.get_data()


# ---------------------------------------------------------------------------
# Whitespace Normalization
# ---------------------------------------------------------------------------

def normalize_whitespace(text: str) -> str:
    """
    Normalisasi semua whitespace:
      - Hapus newline berlebih (lebih dari 1 berturut-turut)
      - Hapus tab
      - Kolaps spasi ganda menjadi satu
      - Strip spasi di awal dan akhir

    Berlaku untuk: semua sumber.
    """
    if not text or not isinstance(text, str):
        return ""

    # Ganti tab dan newline dengan spasi
    text = re.sub(r"[\t\n\r]+", " ", text)
    # Kolaps spasi berlebih
    text = re.sub(r" {2,}", " ", text)
    return text.strip()


# ---------------------------------------------------------------------------
# Noise Removal
# ---------------------------------------------------------------------------

# Karakter yang dianggap noise jika berdiri sendiri atau berulang
# (bukan bagian dari angka, nama, atau satuan)
_NOISE_PATTERN = re.compile(
    r"(?<!\w)[^\w\s,.()%/:\-–](?!\w)",  # simbol non-alfanumerik terisolasi
)

def remove_noise(text: str) -> str:
    """
    Hapus karakter non-informatif yang tidak memberi makna dalam kalimat.
    Contoh: simbol asing, karakter kontrol, encoding artifact.

    Catatan: TIDAK menghapus tanda baca umum (koma, titik, kurung, persen, dll)
    karena masih bermakna dalam konteks statistik.

    Berlaku untuk: Dynamic Data, SIMDASI — data numerik dengan potensi artifact.
    """
    if not text or not isinstance(text, str):
        return ""

    text = _NOISE_PATTERN.sub("", text)
    return normalize_whitespace(text)


# ---------------------------------------------------------------------------
# Symbol Legend (khusus SIMDASI & Static Table)
# ---------------------------------------------------------------------------

# Default mapping simbol BPS ke teks bermakna
DEFAULT_SYMBOL_LEGEND = {
    "..."  : "data tidak tersedia",
    "–"    : "tidak ada (nol)",
    "-"    : "tidak ada (nol)",
    "NA"   : "tidak dapat ditampilkan",
    "e"    : "(estimasi)",
    "r"    : "(diperbaiki)",
    "~0"   : "dapat diabaikan",
    "*"    : "(sementara)",
    "**"   : "(sangat sementara)",
    "***"  : "(sangat sangat sementara)",
    "a"    : "(RSE 25%-50%)",
}

def apply_symbol_legend(value: str, legend: dict = None) -> str:
    """
    Konversi nilai simbol BPS ke teks bermakna.
    Jika `value` adalah simbol yang dikenal, return penjelasannya.
    Jika tidak dikenal sebagai simbol, return value apa adanya.

    Parameter:
      value  : Nilai dari field data (bisa angka string atau simbol)
      legend : Custom mapping, default = DEFAULT_SYMBOL_LEGEND

    Contoh:
      apply_symbol_legend("–")   → "tidak ada (nol)"
      apply_symbol_legend("82.21") → "82.21"
      apply_symbol_legend("*")   → "(sementara)"
    """
    if not isinstance(value, str):
        return str(value) if value is not None else "data tidak tersedia"

    effective_legend = legend if legend is not None else DEFAULT_SYMBOL_LEGEND
    return effective_legend.get(value.strip(), value)


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

REQUIRED_FIELDS = ("id", "content")

def validate_doc(doc: dict) -> bool:
    """
    Validasi minimal sebuah dokumen sebelum disimpan ke MongoDB.
    Cek:
      - Field `id` dan `content` wajib ada dan tidak kosong/None
      - `content` harus berupa string non-kosong

    Return True jika valid, False jika tidak.
    Berlaku untuk: semua sumber sebelum upsert.
    """
    for field in REQUIRED_FIELDS:
        if field not in doc:
            return False
        if doc[field] is None:
            return False
        if isinstance(doc[field], str) and not doc[field].strip():
            return False
    return True


# ---------------------------------------------------------------------------
# ID Generation
# ---------------------------------------------------------------------------

def generate_id(content: str) -> str:
    """
    Generate ID unik deterministik dari content menggunakan MD5 hash.
    Dokumen dengan content identik akan menghasilkan ID yang sama —
    ini yang memungkinkan deduplication otomatis.

    Berlaku untuk: semua sumber.
    """
    if not content or not isinstance(content, str):
        raise ValueError("Content tidak boleh kosong untuk generate_id")

    return hashlib.md5(content.strip().encode("utf-8")).hexdigest()
