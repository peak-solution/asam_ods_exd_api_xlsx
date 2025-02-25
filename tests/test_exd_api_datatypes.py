import logging
import os
import pathlib
import tempfile
import unittest
from pathlib import Path

import numpy as np
import pandas as pd
from google.protobuf.json_format import MessageToJson

import ods_external_data_pb2 as oed
import ods_pb2 as ods
from external_data_reader import ExternalDataReader

# pylint: disable=E1101


class TestDataTypes(unittest.TestCase):
    log = logging.getLogger(__name__)

    def _get_example_file_path(self, file_name):
        example_file_path = pathlib.Path.joinpath(pathlib.Path(
            __file__).parent.resolve(), "..", "data", file_name)
        return pathlib.Path(example_file_path).absolute().resolve().as_uri()

    def test_data_type(self):
        with tempfile.TemporaryDirectory() as temporary_directory_name:
            file_path = os.path.join(
                temporary_directory_name, "all_datatypes_test.xlsx")

            data = {
                "index": [0, 1],
                "complex64": np.array([1 + 2j, 3 + 4j], np.complex64),
                "complex128": np.array([5 + 6j, 7 + 8j], np.complex128),
                "int8": np.array([-2, 4], np.int8),
                "uint8": np.array([2, 4], np.uint8),
                "int16": np.array([-2, 4], np.int16),
                "uint16": np.array([2, 4], np.uint16),
                "int32": np.array([-2, 4], np.int32),
                "uint32": np.array([2, 4], np.uint32),
                "int64": np.array([-2, 4], np.int64),
                "uint64": np.array([2, 4], np.uint64),
                "float32": np.array([1.1, 1.2], np.float32),
                "float64": np.array([2.1, 2.2], np.float64),
                "string": ["abc", "def"],
                "date": np.array([np.datetime64('2023-01-01T11:22:33'), np.datetime64('2023-01-02T11:22:34')], np.datetime64)
            }

            # Convert the dictionary to a DataFrame
            df = pd.DataFrame(data)
            df.to_excel(file_path, index=False)

            service = ExternalDataReader()
            handle = service.Open(oed.Identifier(
                url=Path(file_path).resolve().as_uri(), parameters=""), None)
            try:
                structure = service.GetStructure(
                    oed.StructureRequest(handle=handle), None)
                self.log.info(MessageToJson(structure))

                self.assertEqual(structure.name, "all_datatypes_test.xlsx")
                self.assertEqual(len(structure.groups), 1)
                self.assertEqual(structure.groups[0].number_of_rows, 2)
                self.assertEqual(len(structure.groups[0].channels), 15)

                self.assertEqual(
                    1, structure.groups[0].channels[0].attributes.variables["independent"].long_array.values[0])

                self.assertEqual(
                    structure.groups[0].channels[0].data_type, ods.DataTypeEnum.DT_DOUBLE)
                self.assertEqual(
                    structure.groups[0].channels[1].data_type, ods.DataTypeEnum.DT_STRING)
                self.assertEqual(
                    structure.groups[0].channels[2].data_type, ods.DataTypeEnum.DT_STRING)
                self.assertEqual(
                    structure.groups[0].channels[3].data_type, ods.DataTypeEnum.DT_DOUBLE)
                self.assertEqual(
                    structure.groups[0].channels[4].data_type, ods.DataTypeEnum.DT_DOUBLE)
                self.assertEqual(
                    structure.groups[0].channels[5].data_type, ods.DataTypeEnum.DT_DOUBLE)
                self.assertEqual(
                    structure.groups[0].channels[6].data_type, ods.DataTypeEnum.DT_DOUBLE)
                self.assertEqual(
                    structure.groups[0].channels[7].data_type, ods.DataTypeEnum.DT_DOUBLE)
                self.assertEqual(
                    structure.groups[0].channels[8].data_type, ods.DataTypeEnum.DT_DOUBLE)
                self.assertEqual(
                    structure.groups[0].channels[9].data_type, ods.DataTypeEnum.DT_DOUBLE)
                self.assertEqual(
                    structure.groups[0].channels[10].data_type, ods.DataTypeEnum.DT_DOUBLE)
                self.assertEqual(
                    structure.groups[0].channels[11].data_type, ods.DataTypeEnum.DT_DOUBLE)
                self.assertEqual(
                    structure.groups[0].channels[12].data_type, ods.DataTypeEnum.DT_DOUBLE)
                self.assertEqual(
                    structure.groups[0].channels[13].data_type, ods.DataTypeEnum.DT_STRING)
                self.assertEqual(
                    structure.groups[0].channels[14].data_type, ods.DataTypeEnum.DT_DATE)

                values = service.GetValues(
                    oed.ValuesRequest(
                        handle=handle, group_id=0, start=0, limit=2, channel_ids=[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14]), None
                )
                self.assertSequenceEqual(
                    values.channels[0].values.double_array.values, [0, 1])
                self.assertSequenceEqual(
                    values.channels[1].values.string_array.values, ['(1+2j)', '(3+4j)'])
                self.assertSequenceEqual(
                    values.channels[2].values.string_array.values, ['(5+6j)', '(7+8j)'])
                self.assertSequenceEqual(
                    values.channels[3].values.double_array.values, [-2, 4])
                self.assertSequenceEqual(
                    values.channels[4].values.double_array.values, [2, 4])
                self.assertSequenceEqual(
                    values.channels[5].values.double_array.values, [-2, 4])
                self.assertSequenceEqual(
                    values.channels[6].values.double_array.values, [2, 4])
                self.assertSequenceEqual(
                    values.channels[7].values.double_array.values, [-2, 4])
                self.assertSequenceEqual(
                    values.channels[8].values.double_array.values, [2, 4])
                self.assertSequenceEqual(
                    values.channels[9].values.double_array.values, [-2, 4])
                self.assertSequenceEqual(
                    values.channels[10].values.double_array.values, [2, 4])
                self.assertSequenceEqual(
                    values.channels[11].values.double_array.values, [1.100000023841858, 1.200000047683716])
                self.assertSequenceEqual(
                    values.channels[12].values.double_array.values, [2.1, 2.2])
                self.assertSequenceEqual(
                    values.channels[13].values.string_array.values, ["abc", "def"])
                self.assertSequenceEqual(
                    values.channels[14].values.string_array.values, [
                        '20230101112233000000', '20230102112234000000'])
            finally:
                service.Close(handle, None)

    def test_unit_and_description(self):
        file_url = self._get_example_file_path(
            "example_data_with_unit_and_comment.xlsx")

        service = ExternalDataReader()
        handle = service.Open(oed.Identifier(
            url=file_url, parameters=""), None)
        try:
            structure = service.GetStructure(
                oed.StructureRequest(handle=handle), None)
            self.log.info(MessageToJson(structure))

            self.assertEqual(
                structure.name, "example_data_with_unit_and_comment.xlsx")
            self.assertEqual(len(structure.groups), 1)
            self.assertEqual(structure.groups[0].number_of_rows, 3)
            self.assertEqual(len(structure.groups[0].channels), 4)
            self.assertEqual(
                1, structure.groups[0].channels[0].attributes.variables["independent"].long_array.values[0])
            self.assertEqual("s", structure.groups[0].channels[0].unit_string)
            self.assertEqual("-", structure.groups[0].channels[1].unit_string)
            self.assertEqual(
                "m/s", structure.groups[0].channels[2].unit_string)
            self.assertEqual("-", structure.groups[0].channels[3].unit_string)
            self.assertEqual(
                "time channel", structure.groups[0].channels[0].attributes.variables["description"].string_array.values[0])
            self.assertEqual(
                "index", structure.groups[0].channels[1].attributes.variables["description"].string_array.values[0])
            self.assertEqual(
                "Speed of the vehicle", structure.groups[0].channels[2].attributes.variables["description"].string_array.values[0])
            self.assertEqual(
                "comment column", structure.groups[0].channels[3].attributes.variables["description"].string_array.values[0])

            values = service.GetValues(
                oed.ValuesRequest(
                    handle=handle, group_id=0, start=0, limit=1000, channel_ids=[0, 1, 2, 3]), None
            )
            self.assertSequenceEqual(
                values.channels[0].values.string_array.values, ['20241014114858481000', '20241014114859481000', '20241014114900481000'])
            self.assertSequenceEqual(
                values.channels[1].values.double_array.values, [1, 2, 3])
            self.assertSequenceEqual(
                values.channels[2].values.double_array.values, [3.0, 3.1, 3.2])
            self.assertSequenceEqual(
                values.channels[3].values.string_array.values, ["abc", "def", "ghi"])
        finally:
            service.Close(handle, None)
