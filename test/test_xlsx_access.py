# Prepare python to use GRPC interface:
# python -m grpc_tools.protoc --proto_path=proto_src --pyi_out=. --python_out=. --grpc_python_out=. ods.proto ods_external_data.proto
import sys, os

sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/..")

import logging
import pathlib
import unittest
import tempfile
from datetime import datetime, timedelta

from openpyxl import Workbook


class TestStringMethods(unittest.TestCase):
    log = logging.getLogger(__name__)

    def _get_example_file_path(self, file_name):
        example_file_path = pathlib.Path.joinpath(pathlib.Path(__file__).parent.resolve(), "..", "data", file_name)
        return pathlib.Path(example_file_path).absolute()

    def test_create_file_with_unit(self):
        with tempfile.TemporaryDirectory() as tmp_dir_name:
            file_path = pathlib.Path.joinpath(tmp_dir_name, "example_data_with_unit.xlsx").absolute()
            now = datetime.now()
            wb = Workbook()
            ws = wb.active
            ws.title = "data_1"
            ws.append(["time", "index", "speed", "comment"])
            ws.append(["s", "-", "m/s", "-"])
            ws.append([now + timedelta(seconds=0), 1, 3.0, "abc"])
            ws.append([now + timedelta(seconds=1), 2, 3.1, "def"])
            ws.append([now + timedelta(seconds=2), 3, 3.2, "ghi"])
            wb.save(file_path)

    def test_create_file(self):
        file_path = self._get_example_file_path("example_data.xlsx")
        now = datetime.now()
        wb = Workbook()
        ws = wb.active
        ws.title = "data_1"
        ws.append(["time", "index", "speed", "comment"])
        ws.append([now + timedelta(seconds=0), 1, 3.0, "abc"])
        ws.append([now + timedelta(seconds=1), 2, 3.1, "def"])
        ws.append([now + timedelta(seconds=2), 3, 3.2, "ghi"])
        # Save the file
        wb.save(file_path)

    def test_create_file_multi_sheet(self):
        file_path = self._get_example_file_path("example_data_two_sheets.xlsx")
        now = datetime.now()
        wb = Workbook()
        ws = wb.active
        ws.title = "data_1"
        ws.append(["time", "index", "speed", "comment"])
        ws.append([now + timedelta(seconds=0), 1, 3.0, "abc"])
        ws.append([now + timedelta(seconds=1), 2, 3.1, "def"])
        ws.append([now + timedelta(seconds=2), 3, 3.2, "ghi"])
        ws2 = wb.create_sheet("data_2")
        ws2.append(["time", "index", "speed", "comment"])
        ws2.append([now + timedelta(seconds=3), 4, 4.0, "abc2"])
        ws2.append([now + timedelta(seconds=4), 5, 4.1, "def2"])
        ws2.append([now + timedelta(seconds=5), 6, 4.2, "ghi2"])
        ws2.append([now + timedelta(seconds=6), 7, 4.3, "jkl2"])
        ws2.append([now + timedelta(seconds=7), 8, 4.4, "mno2"])
        wb.save(file_path)


if __name__ == "__main__":
    unittest.main()
