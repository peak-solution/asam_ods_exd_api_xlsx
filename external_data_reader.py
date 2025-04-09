"""
ASAM ODS EXD API implementation for XLSX files
"""

import datetime
import os
from pathlib import Path
import threading
from typing import Tuple
from urllib.parse import urlparse, unquote
from urllib.request import url2pathname

import pandas as pd
import numpy as np
import grpc

# pylint: disable=E1101
import ods_pb2 as ods
import ods_external_data_pb2 as exd_api
import ods_external_data_pb2_grpc


class ExternalDataReader(ods_external_data_pb2_grpc.ExternalDataReader):
    """
    This class implements the ASAM ODS EXD API to read simple XLSX files.
    """

    def Open(self, identifier: exd_api.Identifier, context: grpc.ServicerContext) -> exd_api.Handle:
        """
        Signals an open access to an resource. The server will call `close` later on.

        :param exd_api.Identifier identifier: Contains parameters and file url
        :param dict context: Additional parameters from grpc
        :raises ValueError: If file does not exist
        :return exd_api.Handle: Handle to the opened file.
        """
        file_path = Path(self.__get_path(identifier.url))
        if not file_path.is_file():
            raise ValueError(
                f'File "{identifier.url}" not accessible from plugin.')

        connection_id = self.__open_with_identifier(identifier)

        rv = exd_api.Handle(uuid=connection_id)
        return rv

    def Close(self, handle: exd_api.Handle, context: grpc.ServicerContext) -> exd_api.Empty:
        """
        Close resource opened before and signal the plugin that it is no longer used.

        :param exd_api.Handle handle: Handle to a resource returned before.
        :param dict context: Additional parameters from grpc.
        :return exd_api.Empty: Empty object.
        """
        self.__close_by_handle(handle)
        return exd_api.Empty()

    def __find_stable_datatype_row(self, df):
        previous_dtypes = None
        for index, row in df.iterrows():
            current_dtypes = row.apply(type)
            if row.isnull().all():
                previous_dtypes = None
                continue

            # Replace integer based dtypes with float based
            current_dtypes = current_dtypes.apply(
                lambda dtype: np.float64 if np.issubdtype(dtype, np.integer) or np.issubdtype(dtype, np.floating) else dtype)

            # Check if current row datatypes match the previous row datatypes
            if previous_dtypes is None or not current_dtypes.equals(previous_dtypes):
                previous_dtypes = current_dtypes
            else:
                # Check if at least one column isn't a string
                if any(dtype != str for dtype in current_dtypes):
                    return index - 1  # Return the index of the previous row

        return None  # If no consistent row is found

    def __calculate_mean_of_string_lengths(self, meta_rows):
        mean_lengths = {}
        for index, row in meta_rows.iterrows():
            mean_lengths[index] = None
            if row.apply(lambda x: isinstance(x, str) or pd.isnull(x)).all():
                string_values = row.apply(
                    lambda x: x if isinstance(x, str) else None).dropna()
                if not string_values.empty:
                    mean_length = string_values.apply(len).mean()
                    mean_lengths[index] = mean_length

        return mean_lengths

    def __get_channel_data_type(self, channel) -> ods.DataTypeEnum:
        if np.issubdtype(channel.dtypes, np.bool_):
            return ods.DataTypeEnum.DT_BOOLEAN
        if np.issubdtype(channel.dtypes, np.int8):
            return ods.DataTypeEnum.DT_BYTE
        if np.issubdtype(channel.dtypes, np.int16):
            return ods.DataTypeEnum.DT_SHORT
        if np.issubdtype(channel.dtypes, np.int32):
            return ods.DataTypeEnum.DT_LONG
        if np.issubdtype(channel.dtypes, np.int64):
            # DT_LONGLONG # All int types are mapped to int64, so we pick double
            return ods.DataTypeEnum.DT_DOUBLE
        if np.issubdtype(channel.dtypes, np.float32):
            return ods.DataTypeEnum.DT_FLOAT
        if np.issubdtype(channel.dtypes, np.float64):
            return ods.DataTypeEnum.DT_DOUBLE
        if np.issubdtype(channel.dtypes, np.complex64):
            return ods.DataTypeEnum.DT_COMPLEX
        if np.issubdtype(channel.dtypes, np.complex128):
            return ods.DataTypeEnum.DT_DCOMPLEX
        if np.issubdtype(channel.dtypes, np.datetime64):
            return ods.DataTypeEnum.DT_DATE
        if channel.dtypes == np.object_:
            first_value = channel.iloc[0]
            if isinstance(first_value, datetime.time):
                return ods.DataTypeEnum.DT_DATE
            if isinstance(first_value, str):
                return ods.DataTypeEnum.DT_STRING
            if isinstance(first_value, int):
                return ods.DataTypeEnum.DT_LONGLONG

        raise NotImplementedError(f"Unknown pandas dtype {
            channel.dtypes} for channel {channel.name}")

    def __assign_df_values_to_unknown_sequence(self, section: pd.Series, channel_datatype: ods.DataTypeEnum,
                                               new_channel_values: exd_api.ValuesResult.ChannelValues):
        new_channel_values.values.data_type = channel_datatype
        if channel_datatype == ods.DataTypeEnum.DT_BOOLEAN:
            new_channel_values.values.boolean_array.values.extend(section)
        elif channel_datatype == ods.DataTypeEnum.DT_BYTE:
            new_channel_values.values.byte_array.values = section.tobytes()
        elif channel_datatype == ods.DataTypeEnum.DT_SHORT:
            new_channel_values.values.long_array.values[:] = section
        elif channel_datatype == ods.DataTypeEnum.DT_LONG:
            new_channel_values.values.long_array.values[:] = section
        elif channel_datatype == ods.DataTypeEnum.DT_LONGLONG:
            new_channel_values.values.longlong_array.values[:] = pd.to_numeric(
                section, errors='coerce').fillna(0).astype(np.int64)
        elif channel_datatype == ods.DataTypeEnum.DT_FLOAT:
            new_channel_values.values.float_array.values[:] = section
        elif channel_datatype == ods.DataTypeEnum.DT_DOUBLE:
            new_channel_values.values.double_array.values[:] = pd.to_numeric(
                section, errors='coerce').astype(np.float64)
        elif channel_datatype == ods.DataTypeEnum.DT_DATE:
            if isinstance(section.iloc[0], datetime.time):
                values = [
                    datetime.datetime.combine(datetime.date(1970, 1, 1), t).strftime('%Y%m%d%H%M%S%f')
                    if pd.notnull(t) else ""
                    for t in section
                ]
                new_channel_values.values.string_array.values[:] = values
            else:
                new_channel_values.values.string_array.values[:] = section.dt.strftime(
                    '%Y%m%d%H%M%S%f')
        elif channel_datatype == ods.DataTypeEnum.DT_COMPLEX:
            real_values = []
            for complex_value in section:
                real_values.append(complex_value.real)
                real_values.append(complex_value.imag)
            new_channel_values.values.float_array.values[:] = real_values
        elif channel_datatype == ods.DataTypeEnum.DT_DCOMPLEX:
            real_values = []
            for complex_value in section:
                real_values.append(complex_value.real)
                real_values.append(complex_value.imag)
            new_channel_values.values.double_array.values[:] = real_values
        elif channel_datatype == ods.DataTypeEnum.DT_STRING:
            new_channel_values.values.string_array.values[:] = section.fillna(
                "").astype(dtype="str")
        elif channel_datatype == ods.DataTypeEnum.DT_BYTESTR:
            for item in section:
                new_channel_values.values.bytestr_array.values.append(
                    item.tobytes())
        else:
            raise NotImplementedError(
                f"Unknown np datatype {
                    section.dtype} for type {channel_datatype}!"
            )

    def GetStructure(self, structure_request: exd_api.StructureRequest, context: grpc.ServicerContext) -> exd_api.StructureResult:
        """
        Get the structure of the file returned as file-group-channel hierarchy.

        :param exd_api.StructureRequest structure_request: Defines what to extract from the file structure.
        :param dict context: Additional parameters from grpc.
        :raises NotImplementedError: If advanced features are requested.
        :return exd_api.StructureResult: The structure of the opened file.
        """
        if (
            structure_request.suppress_channels
            or structure_request.suppress_attributes
            or 0 != len(structure_request.channel_names)
        ):
            context.set_code(grpc.StatusCode.UNIMPLEMENTED)
            context.set_details("Method not implemented!")
            raise NotImplementedError("Method not implemented!")

        identifier = self.connection_map[structure_request.handle.uuid]
        xlsx = self.__get_by_handle(structure_request.handle)

        rv = exd_api.StructureResult(identifier=identifier)
        rv.name = Path(identifier.url).name
        # rv.attributes.variables["start_time"].string_array.values.append(
        #     xlsx.start_time.strftime("%Y%m%d%H%M%S%f"))

        for sheet_index, sheet_name in enumerate(xlsx.sheet_names, start=0):

            meta_df, data_df = self.__load_sheet(context, xlsx, sheet_index)

            column_descriptions = None
            column_units = None
            mean_of_string_length = self.__calculate_mean_of_string_lengths(
                meta_df)
            if len(mean_of_string_length) > 0:
                for index, row in meta_df.iterrows():
                    if mean_of_string_length[index] is not None:
                        if mean_of_string_length[index] < 5:
                            column_units = row
                        else:
                            column_descriptions = row

            new_group = exd_api.StructureResult.Group()
            new_group.name = sheet_name
            new_group.id = sheet_index
            new_group.total_number_of_channels = len(data_df.columns)
            new_group.number_of_rows = int(data_df.shape[0])

            for column_index, column in enumerate(data_df.columns, start=0):
                new_channel = exd_api.StructureResult.Channel()
                new_channel.name = column
                new_channel.id = column_index
                column_unit = column_units.iloc[column_index] if column_units is not None else None
                column_description = column_descriptions.iloc[
                    column_index] if column_descriptions is not None else None

                new_channel.data_type = self.__get_channel_data_type(
                    data_df[column])
                if column_unit is not None and not pd.isna(column_unit):
                    new_channel.unit_string = column_unit
                if column_description is not None and not pd.isna(column_description):
                    new_channel.attributes.variables["description"].string_array.values.append(
                        column_description)
                if 0 == column_index and data_df.iloc[:, column_index].is_monotonic_increasing:
                    new_channel.attributes.variables["independent"].long_array.values.append(
                        1)
                new_group.channels.append(new_channel)

            rv.groups.append(new_group)

        return rv

    def __load_sheet(self, context, xlsx: pd.ExcelFile, sheet_index: int) -> Tuple[pd.DataFrame, pd.DataFrame]:
        sheet_df = pd.read_excel(xlsx, sheet_name=sheet_index)
        first_stable_row = self.__find_stable_datatype_row(sheet_df)
        if first_stable_row is None:
            context.set_details(
                f"No stable row found in sheet {xlsx.sheet_names[sheet_index]}!")
            context.abort_with_status(status=grpc.StatusCode.OUT_OF_RANGE)

        meta_df = pd.DataFrame(
            columns=sheet_df.columns) if first_stable_row == 0 else sheet_df.iloc[:first_stable_row].copy()

        data_df = None
        if 0 == first_stable_row:
            data_df = sheet_df
        else:
            data_df = sheet_df.drop(range(first_stable_row))
            data_df.reset_index(drop=True, inplace=True)
            data_df = data_df.infer_objects()
        return meta_df, data_df

    def GetValues(self, values_request: exd_api.ValuesRequest, context: grpc.ServicerContext) -> exd_api.ValuesResult:
        """
        Retrieve channel/signal data identified by `values_request`.

        :param exd_api.ValuesRequest values_request: Defines the group and its channels to be retrieved.
        :param dict context: Additional grpc parameters.
        :raises NotImplementedError: If unknown data type is accessed.
        :return exd_api.ValuesResult: The chunk of bulk data.
        """
        xlsx = self.__get_by_handle(values_request.handle)

        if values_request.group_id < 0 or values_request.group_id >= len(xlsx.sheet_names):
            context.set_details(f"Invalid group id {values_request.group_id}!")
            context.abort_with_status(status=grpc.StatusCode.OUT_OF_RANGE)

        _, bulk_df = self.__load_sheet(
            context, xlsx, values_request.group_id)

        nr_of_rows = bulk_df.shape[0]
        if values_request.start > nr_of_rows:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(f"Channel start index {
                                values_request.start} out of range!")
            raise NotImplementedError(f"Channel start index {
                                      values_request.start} out of range!")

        end_index = min(values_request.start +
                        values_request.limit, nr_of_rows)

        rv = exd_api.ValuesResult(id=values_request.group_id)
        for channel_id in values_request.channel_ids:
            if channel_id >= bulk_df.shape[1]:
                context.set_details(f"Invalid channel id {channel_id}!")
                context.abort_with_status(status=grpc.StatusCode.OUT_OF_RANGE)

            section = bulk_df.iloc[values_request.start: end_index, channel_id]

            channel_datatype = self.__get_channel_data_type(section)

            new_channel_values = exd_api.ValuesResult.ChannelValues()
            new_channel_values.id = channel_id

            self.__assign_df_values_to_unknown_sequence(
                section, channel_datatype, new_channel_values)

            rv.channels.append(new_channel_values)

        return rv

    def GetValuesEx(self, request: exd_api.ValuesExRequest, context: grpc.ServicerContext) -> exd_api.ValuesExResult:
        """
        Method to access virtual groups and channels. Currently not supported by the plugin

        :param exd_api.ValuesExRequest request: Defines virtual groups and channels to be accessed.
        :param dict context: Additional grpc parameters.
        :raises NotImplementedError: Currently not implemented. Only needed for very advanced use.
        :return exd_api.ValuesExResult: Bulk values requested.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")

    def __init__(self):
        self.connect_count = 0
        self.connection_map = {}
        self.file_map = {}
        self.lock = threading.Lock()

    def __get_id(self, identifier: exd_api.Identifier) -> str:
        self.connect_count = self.connect_count + 1
        rv = str(self.connect_count)
        self.connection_map[rv] = identifier
        return rv

    def __uri_to_path(self, uri: str) -> str:
        parsed = urlparse(uri)
        host = f"{os.path.sep}{os.path.sep}{parsed.netloc}{os.path.sep}"
        return os.path.normpath(os.path.join(host, url2pathname(unquote(parsed.path))))

    def __get_path(self, file_url: str) -> str:
        final_path = self.__uri_to_path(file_url)
        return final_path

    def __open_with_identifier(self, identifier: exd_api.Identifier):
        with self.lock:
            identifier.parameters  # might be used in the future
            connection_id = self.__get_id(identifier)
            connection_url = self.__get_path(identifier.url)
            if connection_url not in self.file_map:
                self.file_map[connection_url] = {
                    "xlsx": pd.ExcelFile(connection_url),
                    "ref_count": 0
                }
            self.file_map[connection_url]["ref_count"] = self.file_map[connection_url]["ref_count"] + 1
            return connection_id

    def __get_by_handle(self, handle: exd_api.Handle) -> pd.ExcelFile:
        identifier = self.connection_map[handle.uuid]
        connection_url = self.__get_path(identifier.url)
        return self.file_map[connection_url]["xlsx"]

    def __close_by_handle(self, handle: exd_api.Handle):
        with self.lock:
            identifier = self.connection_map[handle.uuid]
            connection_url = self.__get_path(identifier.url)
            if self.file_map[connection_url]["ref_count"] > 1:
                self.file_map[connection_url]["ref_count"] = self.file_map[connection_url]["ref_count"] - 1
            else:
                self.file_map[connection_url]["xlsx"].close()
                del self.file_map[connection_url]


if __name__ == "__main__":

    from google.protobuf.json_format import MessageToJson

    external_data_reader = ExternalDataReader()

    exd_api_handle = external_data_reader.Open(
        exd_api.Identifier(
            # url="file:///workspaces/asam_ods_exd_api_xlsx/data/example_data_with_unit_and_comment.xlsx"), None
            url="file:///workspaces/asam_ods_exd_api_xlsx/data/datetime_date.xlsx"), None
    )
    exd_api_request = exd_api.StructureRequest(handle=exd_api_handle)
    exd_api_structure = external_data_reader.GetStructure(
        exd_api_request, None)
    print(MessageToJson(exd_api_structure))

    # loop over all channels and read values
    for group in exd_api_structure.groups:
        channel_ids = [channel.id for channel in group.channels]
        if len(channel_ids) > 0:
            exd_api_values = external_data_reader.GetValues(
                exd_api.ValuesRequest(
                    handle=exd_api_handle,
                    group_id=group.id,
                    channel_ids=channel_ids,
                    start=0,
                    limit=group.number_of_rows), None)

    print(
        MessageToJson(
            external_data_reader.GetValues(
                exd_api.ValuesRequest(
                    handle=exd_api_handle, group_id=0, channel_ids=[0, 1], start=0, limit=100
                ),
                None,
            )
        )
    )
