"""
test_ingestion_day6.py
Unit tests untuk Fase 1A Hari 6:
  - cleaner.py (semua fungsi)
  - dynamic_preprocessor.py (decode + konversi kalimat)

Jalankan:
  pytest tests/test_ingestion_day6.py -v
"""

import pytest
from ingestion.cleaner import (
    clean_html,
    normalize_whitespace,
    remove_noise,
    apply_symbol_legend,
    validate_doc,
    generate_id,
)
from ingestion.dynamic_preprocessor import (
    build_lookup_maps,
    decode_datacontent_key,
    build_sentence,
)


# ===========================================================================
# Tests: cleaner.py
# ===========================================================================

class TestCleanHtml:
    def test_strip_basic_tags(self):
        assert clean_html("<p>Bandung</p>") == "Bandung"

    def test_decode_html_entities(self):
        assert "&amp;" not in clean_html("Bandung &amp; sekitarnya")
        assert "&" in clean_html("Bandung &amp; sekitarnya")

    def test_strip_ul_li(self):
        html = "<ul><li>Poin satu</li><li>Poin dua</li></ul>"
        result = clean_html(html)
        assert "Poin satu" in result
        assert "Poin dua" in result
        assert "<" not in result

    def test_empty_string(self):
        assert clean_html("") == ""

    def test_none_input(self):
        assert clean_html(None) == ""

    def test_plain_text_unchanged(self):
        assert clean_html("Teks biasa") == "Teks biasa"


class TestNormalizeWhitespace:
    def test_collapse_double_spaces(self):
        assert normalize_whitespace("kata  kata") == "kata kata"

    def test_remove_newlines(self):
        assert "\n" not in normalize_whitespace("baris satu\nbaris dua")

    def test_remove_tabs(self):
        assert "\t" not in normalize_whitespace("kolom\tkolom")

    def test_strip_edges(self):
        assert normalize_whitespace("  spasi  ") == "spasi"

    def test_empty_string(self):
        assert normalize_whitespace("") == ""


class TestApplySymbolLegend:
    def test_dash_symbol(self):
        result = apply_symbol_legend("–")
        assert "nol" in result or "tidak ada" in result

    def test_ellipsis_symbol(self):
        result = apply_symbol_legend("...")
        assert "tidak tersedia" in result

    def test_na_symbol(self):
        result = apply_symbol_legend("NA")
        assert "tidak dapat" in result

    def test_asterisk_symbol(self):
        result = apply_symbol_legend("*")
        assert "sementara" in result

    def test_numeric_string_unchanged(self):
        assert apply_symbol_legend("82.21") == "82.21"

    def test_custom_legend(self):
        custom = {"X": "nilai khusus"}
        assert apply_symbol_legend("X", legend=custom) == "nilai khusus"

    def test_none_value(self):
        result = apply_symbol_legend(None)
        assert "tidak tersedia" in result


class TestValidateDoc:
    def test_valid_doc(self):
        doc = {"id": "abc123", "content": "Teks valid"}
        assert validate_doc(doc) is True

    def test_missing_id(self):
        doc = {"content": "Teks valid"}
        assert validate_doc(doc) is False

    def test_missing_content(self):
        doc = {"id": "abc123"}
        assert validate_doc(doc) is False

    def test_empty_content(self):
        doc = {"id": "abc123", "content": ""}
        assert validate_doc(doc) is False

    def test_none_content(self):
        doc = {"id": "abc123", "content": None}
        assert validate_doc(doc) is False

    def test_whitespace_only_content(self):
        doc = {"id": "abc123", "content": "   "}
        assert validate_doc(doc) is False


class TestGenerateId:
    def test_same_content_same_id(self):
        assert generate_id("teks sama") == generate_id("teks sama")

    def test_different_content_different_id(self):
        assert generate_id("teks A") != generate_id("teks B")

    def test_returns_string(self):
        assert isinstance(generate_id("teks"), str)

    def test_md5_length(self):
        # MD5 hex digest selalu 32 karakter
        assert len(generate_id("teks")) == 32

    def test_empty_raises(self):
        with pytest.raises(ValueError):
            generate_id("")

    def test_whitespace_stripped_before_hash(self):
        # Whitespace di tepi tidak mempengaruhi hash
        assert generate_id("  teks  ") == generate_id("teks")


# ===========================================================================
# Tests: dynamic_preprocessor.py
# ===========================================================================

# Fixture: mock raw API response (berdasarkan contoh di Dokumentasi_API.pdf)
MOCK_RAW_DATA = {
    "status": "OK",
    "data-availability": "available",
    "last_update": "2022-01-13 02:25:36",
    "data": {
        "var": [{"val": 133, "label": "Persentase Penggunaan TI", "unit": "Persen"}],
        "turvar": [
            {"val": 115, "label": "Menggunakan Telepon Seluler"},
            {"val": 116, "label": "Mengakses Internet"},
        ],
        "vervar": [
            {"val": 1, "label": "Laki-laki"},
            {"val": 2, "label": "Perempuan"},
            {"val": 3, "label": "Kabupaten Bandung"},
        ],
        "tahun": [{"val": 121, "label": "2021"}],
        "turtahun": [],
        "datacontent": {
            "11331151210": 82.21,
            "11331161210": 76.73,
            "21331151210": 73.5,
            "21331161210": 64.59,
        },
    }
}


class TestBuildLookupMaps:
    def test_vervar_map_populated(self):
        vervar_map, _, _ = build_lookup_maps(MOCK_RAW_DATA)
        assert "1" in vervar_map
        assert vervar_map["1"] == "Laki-laki"

    def test_turvar_map_populated(self):
        _, turvar_map, _ = build_lookup_maps(MOCK_RAW_DATA)
        assert "115" in turvar_map
        assert turvar_map["115"] == "Menggunakan Telepon Seluler"

    def test_tahun_map_fallback(self):
        # turtahun kosong → fallback ke field `tahun`
        _, _, tahun_map = build_lookup_maps(MOCK_RAW_DATA)
        assert "121" in tahun_map
        assert tahun_map["121"] == "2021"

    def test_empty_response(self):
        vervar_map, turvar_map, tahun_map = build_lookup_maps({"data": {}})
        assert vervar_map == {}
        assert turvar_map == {}
        assert tahun_map  == {}


class TestDecodeDatacontentKey:
    def setup_method(self):
        self.vervar_map, self.turvar_map, self.tahun_map = build_lookup_maps(MOCK_RAW_DATA)
        self.var_id = 133

    def test_decode_laki_telepon(self):
        result = decode_datacontent_key(
            "11331151210", self.var_id,
            self.vervar_map, self.turvar_map, self.tahun_map
        )
        assert result is not None
        vervar_label, turvar_label, tahun_label = result
        assert vervar_label == "Laki-laki"
        assert turvar_label == "Menggunakan Telepon Seluler"
        assert tahun_label  == "2021"

    def test_decode_perempuan_internet(self):
        result = decode_datacontent_key(
            "21331161210", self.var_id,
            self.vervar_map, self.turvar_map, self.tahun_map
        )
        assert result is not None
        vervar_label, turvar_label, tahun_label = result
        assert vervar_label == "Perempuan"
        assert turvar_label == "Mengakses Internet"

    def test_invalid_key_returns_none(self):
        result = decode_datacontent_key(
            "99999", self.var_id,
            self.vervar_map, self.turvar_map, self.tahun_map
        )
        assert result is None


class TestBuildSentence:
    def test_sentence_contains_all_components(self):
        sentence = build_sentence(
            title        = "Persentase Penggunaan TI",
            definition   = "Menggunakan Telepon Seluler atau Komputer",
            vervar_label = "Laki-laki",
            turvar_label = "Menggunakan Telepon Seluler",
            tahun_label  = "2021",
            value        = 82.21,
            unit         = "Persen",
        )
        assert "Laki-laki" in sentence
        assert "2021" in sentence
        assert "82" in sentence          # nilai muncul
        assert "Persen" in sentence
        assert "Kota Bandung" in sentence

    def test_sentence_is_string(self):
        sentence = build_sentence("Judul", "Def", "Laki-laki", "TI", "2021", 50.0, "%")
        assert isinstance(sentence, str)

    def test_no_double_spaces(self):
        sentence = build_sentence("Judul", "", "Perempuan", "Internet", "2021", 64.59, "Persen")
        assert "  " not in sentence

    def test_definition_included_when_present(self):
        sentence = build_sentence("Judul", "Definisi penting", "L", "T", "2021", 1.0, "%")
        assert "Definisi penting" in sentence

    def test_definition_skipped_when_empty(self):
        sentence = build_sentence("Judul", "", "L", "T", "2021", 1.0, "%")
        assert sentence.count("Judul") == 1   # judul tidak duplikat
