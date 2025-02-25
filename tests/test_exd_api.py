# Prepare python to use GRPC interface:
# python -m grpc_tools.protoc --proto_path=proto_src --pyi_out=. --python_out=. --grpc_python_out=. ods.proto ods_external_data.proto
import logging
import pathlib
import unittest

from google.protobuf.json_format import MessageToJson

import ods_external_data_pb2 as oed
import ods_pb2 as ods
from external_data_reader import ExternalDataReader

# pylint: disable=E1101


class TestExdApi(unittest.TestCase):
    log = logging.getLogger(__name__)

    def _get_example_file_path(self, file_name):
        example_file_path = pathlib.Path.joinpath(pathlib.Path(
            __file__).parent.resolve(), "..", "data", file_name)
        return pathlib.Path(example_file_path).absolute().as_uri()

    def test_open(self):
        service = ExternalDataReader()
        handle = service.Open(oed.Identifier(
            url=self._get_example_file_path("example_data_with_unit.xlsx"), parameters=""), None)
        try:
            pass
        finally:
            service.Close(handle, None)

    def test_structure(self):
        service = ExternalDataReader()
        handle = service.Open(oed.Identifier(
            url=self._get_example_file_path("example_data_with_unit.xlsx"), parameters=""), None)
        try:
            structure = service.GetStructure(
                oed.StructureRequest(handle=handle), None)
            self.assertEqual(structure.name, "example_data_with_unit.xlsx")
            self.assertEqual(len(structure.groups), 1)
            self.assertEqual(structure.groups[0].number_of_rows, 3)
            self.assertEqual(len(structure.groups[0].channels), 4)
            self.assertEqual(structure.groups[0].id, 0)
            self.log.info(MessageToJson(structure))
            self.assertEqual(structure.groups[0].channels[0].id, 0)
            self.assertEqual(structure.groups[0].channels[0].unit_string, "s")
            self.assertEqual(structure.groups[0].channels[0].name, "time")
        finally:
            service.Close(handle, None)

    def test_get_values(self):
        service = ExternalDataReader()
        handle = service.Open(oed.Identifier(
            url=self._get_example_file_path("example_data_with_unit.xlsx"), parameters=""), None)
        try:
            values = service.GetValues(
                oed.ValuesRequest(handle=handle, group_id=0, channel_ids=[
                                  0, 1, 2, 3], start=0, limit=4), None
            )
            self.assertEqual(values.id, 0)
            self.assertEqual(len(values.channels), 4)
            self.assertEqual(values.channels[0].id, 0)
            self.assertEqual(values.channels[1].id, 1)
            self.log.info(MessageToJson(values))

            self.assertEqual(
                values.channels[0].values.data_type, ods.DataTypeEnum.DT_DATE)
            self.assertSequenceEqual(
                values.channels[0].values.string_array.values, [
                    '20241001101931600000', '20241001101932600000', '20241001101933600000']
            )
            self.assertEqual(
                values.channels[1].values.data_type, ods.DataTypeEnum.DT_DOUBLE)
            self.assertSequenceEqual(
                values.channels[1].values.double_array.values,
                [1, 2, 3],
            )

        finally:
            service.Close(handle, None)
