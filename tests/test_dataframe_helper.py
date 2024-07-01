from harley import column_to_list
import pytest
from datetime import datetime
from tests.conftest import polars_frames

floats=[4.0, 5.0, 6.0, 7.0, 8.0]
data = {
    "integer": [1, 2, 3, 4, 5],
    "date": [
        datetime(2022, 1, 1),
        datetime(2022, 1, 2),
        datetime(2022, 1, 3),
        datetime(2022, 1, 4),
        datetime(2022, 1, 5),
    ],
    "float": floats,
}


@pytest.mark.parametrize("frame_type", polars_frames)
def test_column_to_list(frame_type:str):
    frame = frame_type(data)
    exp = floats
    res = column_to_list(frame, "float")
    assert exp==res
