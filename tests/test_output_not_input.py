import pyarrow as pa
import pyarrow.csv as pacsv
import pytest

from parq.reader import MultiFormatReader, merge_files


def test_merge_force_refuses_to_overwrite_an_input(tmp_path):
    left = tmp_path / "a.csv"
    right = tmp_path / "b.csv"
    pacsv.write_csv(pa.table({"id": [1]}), left)
    pacsv.write_csv(pa.table({"id": [2]}), right)

    with pytest.raises(ValueError, match="must not be one of the inputs"):
        merge_files([left, right], left, force=True)
    # The input must survive (previously --force unlinked it before reading).
    assert pacsv.read_csv(left).num_rows == 1


def test_convert_force_refuses_to_overwrite_its_input(tmp_path):
    src = tmp_path / "a.csv"
    pacsv.write_csv(pa.table({"id": [1, 2]}), src)

    with pytest.raises(ValueError, match="must not be one of the inputs"):
        MultiFormatReader(str(src)).convert_file(src, force=True)
    assert pacsv.read_csv(src).num_rows == 2


def test_merge_force_keeps_existing_output_when_schemas_conflict(tmp_path):
    """A failed schema check must not delete the previous output."""
    left = tmp_path / "a.csv"
    right = tmp_path / "b.csv"
    output = tmp_path / "out.csv"
    pacsv.write_csv(pa.table({"id": [1]}), left)
    pacsv.write_csv(pa.table({"id": ["x"]}), right)
    pacsv.write_csv(pa.table({"id": [9]}), output)

    with pytest.raises(ValueError, match="Incompatible schemas"):
        merge_files([left, right], output, force=True)

    assert pacsv.read_csv(output).column("id").to_pylist() == [9]


def test_convert_force_keeps_existing_output_when_write_fails(tmp_path, monkeypatch):
    """--force replaces the destination only after the new file is complete."""
    from parq.formats import _chunk_writers

    source = tmp_path / "in.csv"
    output = tmp_path / "out.csv"
    pacsv.write_csv(pa.table({"id": [1, 2]}), source)
    output.write_text("keep-me\n", encoding="utf-8")

    def fail_write(self, batch):
        del self, batch
        raise RuntimeError("simulated write failure")

    monkeypatch.setattr(_chunk_writers._CsvChunkWriter, "write_batch", fail_write)

    with pytest.raises(RuntimeError, match="simulated write failure"):
        MultiFormatReader(str(source)).convert_file(output, force=True)

    assert output.read_text(encoding="utf-8") == "keep-me\n"
    assert list(tmp_path.glob(".out.csv.*")) == []
