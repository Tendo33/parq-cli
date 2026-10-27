import zipfile

import pytest

pytest.importorskip("defusedxml")

from parq.formats._xlsx import _active_sheet_index  # noqa: E402

_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"


def _workbook(tmp_path, workbook_xml: str):
    path = tmp_path / "book.xlsx"
    with zipfile.ZipFile(path, "w") as zf:
        zf.writestr("xl/workbook.xml", workbook_xml)
    return path


def test_active_sheet_index_reads_active_tab(tmp_path):
    xml = f'<workbook xmlns="{_NS}"><bookViews><workbookView activeTab="2"/></bookViews></workbook>'
    assert _active_sheet_index(_workbook(tmp_path, xml)) == 2


def test_active_sheet_index_rejects_entity_expansion(tmp_path):
    xml = (
        '<?xml version="1.0"?>'
        '<!DOCTYPE workbook [<!ENTITY a "1"><!ENTITY b "&a;&a;&a;&a;&a;&a;&a;&a;">]>'
        f'<workbook xmlns="{_NS}"><bookViews><workbookView activeTab="&b;"/></bookViews></workbook>'
    )
    # defusedxml refuses entity declarations; we fall back to the first sheet.
    assert _active_sheet_index(_workbook(tmp_path, xml)) == 0
