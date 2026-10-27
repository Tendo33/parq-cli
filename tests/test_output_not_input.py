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
