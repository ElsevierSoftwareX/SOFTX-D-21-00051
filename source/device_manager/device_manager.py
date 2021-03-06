from typing import List, Dict
from uuid import UUID, uuid4
from datetime import datetime
import requests
import timeit
import os

from source.device_manager.device_layer.database_info import DatabaseInfo, DatabaseStatus
from source.device_manager.sila_auto_discovery.sila_auto_discovery import find
from source.device_manager.device_layer.device_info import DeviceInfo, DeviceStatus
from source.device_manager.device_layer.device_interface import DeviceInterface, DeviceType, DeviceError
from source.device_manager.device_layer.dummy_device import DummyDevice
from source.device_manager.device_layer.sila_device import SilaDevice

from source.device_manager.device_layer.device_feature import Feature, FeatureForDataHandler, \
    CommandForDataHandler, CommandResponseForDataHandler, IntermediateCommandResponseForDataHandler, \
    PropertyResponseForDataHandler, PropertyForDataHandler, CommandParameterForDataHandler

from source.device_manager.device_layer.sila_feature import serialize_feature
from source.device_manager.device_log import DeviceManagerLogHandler, LogLevel
from source.device_manager.database import get_database_connection, release_database_connection
from source.device_manager.scheduler import BookingInfo, get_booking_entry, get_device_booking_info, get_booking_info, book, id_is_valid, delete_booking_entry
from source.device_manager.scheduler import BookingInfoWithNames, get_device_booking_info_with_names, get_booking_info_with_names
from source.device_manager.device_layer.dynamic_client import delete_dynamic_client
import source.device_manager.device
import source.device_manager.experiment as experiment
import source.device_manager.script as script

from sila2lib.fdl_parser.fdl_parser import FDLParser
from dataclasses import asdict
from influxdb import InfluxDBClient, exceptions
from multiprocessing import Process, Pipe
from multiprocessing import pool as mpp

import logging

logger = logging.getLogger()
logger.addHandler(DeviceManagerLogHandler(logging.WARNING))

INTERVAL = 30
META_INTERVAL = 3600
ACTIVE = True
META = False


def _call_feature_command_from_subprocess(info: DeviceInfo, qualified_feature_identifier: str,
                                          command_id: str, parameters: Dict[str,
                                                                         any],
                                          connection):
    try:
        device = _create_device_instance(info.address, info.port, info.uuid,
                                         info.name, info.type)
        device.connect()
        # print(qualified_feature_identifier)
        # print(parameters)
        try:
            result = device.call_command(qualified_feature_identifier, command_id, parameters)
        except:
            print('+++++++++++++++++++++++++++++++++++++++++++++++++', qualified_feature_identifier.split('/')[-2])
            result = device.call_command(qualified_feature_identifier.split('/')[-2], command_id, parameters)

        connection.send(result)
    finally:
        connection.close()


def _get_feature_property_from_subprocess(info: DeviceInfo, qualified_feature_identifier: str,
                                          property_id: str, connection):
    try:
        device = _create_device_instance(info.address, info.port, info.uuid,
                                         info.name, info.type)
        device.connect()
        # result = device.call_property(feature+'\n', prop)
        try:
            result = device.call_property(qualified_feature_identifier, property_id)
        except:
            result = device.call_property(qualified_feature_identifier.split('/')[-2], property_id)
        connection.send(result)
    finally:
        connection.close()


def _create_device_instance(ip: str, port: int, uuid: UUID, name: str, type: DeviceType):
    if type == DeviceType.SILA:
        return SilaDevice(ip, port, uuid, name)
    else:
        return DummyDevice(ip, port, uuid, name, type)


def _get_device_instance_from_subprocess(info: DeviceInfo, connection):
    """Get a device instance for the device specified by the provided details
    """
    try:
        device = _create_device_instance(info.address, info.port, info.uuid, info.name, info.type)
        connection.send(device)
    finally:
        connection.close()


def _get_device_status_from_subprocess(info: DeviceInfo, connection):
    """Get the current status of the specified device
    """
    try:
        device = _create_device_instance(info.address, info.port, info.uuid,
                                         info.name, info.type)
        device.connect()
        connection.send(DeviceStatus(device.is_online(), device.get_status()))
    finally:
        connection.close()


def _get_device_features_from_subprocess(info: DeviceInfo, connection):
    """Get the description of supported features of the specified device
    """
    try:
        device = _create_device_instance(info.address, info.port, info.uuid,
                                         info.name, info.type)
        device.connect()
        features = []
        if device.is_online() and device.type == DeviceType.SILA:
            for name in device.get_feature_names():
                if '/' in name:
                    originator, category, feature_identifier, major_feature_version = name.split('/')
                    fdl_filename = os.path.join(originator.strip(),
                                                category.strip(),
                                                feature_identifier.strip(),
                                                major_feature_version.strip(),
                                                f'{feature_identifier.strip()}')
                    feature_file = device.get_feature_path(fdl_filename)
                else:
                    feature_file = device.get_feature_path(name)
                parser = FDLParser(feature_file)
                feature = serialize_feature(parser)
                print(feature)
                features.append(serialize_feature(parser))
        connection.send(features)
    finally:
        connection.close()


def _get_database_status_from_subprocess(info: DatabaseInfo, connection):
    """Get the current status of the specified database
    """
    # Todo: Implement this function
    # db_name = 'schedulerDB'
    # db_username='schedulerApplication',
    # db_password='DigInBio'
    _client = InfluxDBClient(host=info.address, port=info.port, username=info.username, password=info.password,
                             database=info.name)
    print('Database Name', info.name)
    try:
        # Todo: Allow the use of username and password in the future (From the frontend to here)
        # connection = InfluxDBClient(host=host, port=port, username=username, password=password, database=database)
        version = _client.ping()
        ping = timeit.timeit(stmt='def ping(): _client.ping()')
        retention_policy = _client.get_list_retention_policies()
        print(ping, version, retention_policy)
        connection.send(DatabaseStatus(True, f'v.{version}'))
        # Todo: Switch this to implementation below once new DatabaseInfo class is implemented
        # connection.send(DatabaseStatus(True, retention_policy, '', ping, f'v.{version}'))
    except (requests.ConnectionError, requests.HTTPError, requests.Timeout, exceptions.InfluxDBClientError,
            exceptions.InfluxDBServerError) as e:
        print(e)
        err = type(e).__name__
        print('ERROR MESSAGE', err)
        connection.send(DatabaseStatus(False, err))
        # Todo: Switch this to implementation below once new DatabaseInfo class is implemented
        # connection.send(DatabaseStatus(False, '', err, None, ''))
    finally:
        connection.close()


class DeviceManager:
    """ Device Manager Implementation"""
    def __init__(self):
        pass

    def get_device_info_list(self) -> List[DeviceInfo]:
        """Returns a list of devices information from the database"""
        return source.device_manager.device.get_device_info_list()


    def get_device_info(self, uuid: UUID) -> DeviceInfo:
        """Returns the specified device info
        Args:
            uuid (uuid.UUID): The unique id of the device
        Returns:
            DeviceInterface: A instantiated Device
        """
        return source.device_manager.device.get_device_info(uuid)

    def set_device(self, device: DeviceInfo):
        """Updates a device in the database
        Args:
            device: The device that should replace the one in the database
        """
        source.device_manager.device.set_device(device)

    def add_device(self, server_uuid: UUID, name: str, type: DeviceType, address: str, port: int):
        """Add a new device to the database
        Args:
            device: The new device that should be added to the database
        """
        uuid = source.device_manager.device.add_device(server_uuid, name, type, address,
                                                       port)
        self.add_features_for_data_handler(uuid)

    def delete_device(self, uuid: UUID):
        """Delete a device from the database
        Args:
            uuid (uuid.UUID): The unique id of the device
        """
        dev_info = self.get_device_info(uuid)
        source.device_manager.device.delete_device(dev_info.uuid, dev_info.server_uuid)
        self.delete_features(uuid)

    def get_status(self, uuid: UUID) -> DeviceStatus:
        """Get the current status of the specified device
        Args:
            uuid (uuid.UUID): The unique id of the device
        """
        device_info = self.get_device_info(uuid)
        parent_conn, child_conn = Pipe()
        process = Process(target=_get_device_status_from_subprocess,
                          args=(device_info, child_conn),daemon=True)
        device_status = None
        try:
            process.start()
            device_status = parent_conn.recv()
            process.join()
        finally:
            process.close()
            print('get_status process finished')
        return device_status

    def get_device_instance(self, uuid: UUID):
        """Get a device instance for the specified device
        Args:
        uuid (uuid.UUID): The unique id of the device
        """
        device_info = self.get_device_info(uuid)
        parent_conn, child_conn = Pipe()
        process = Process(target=_get_device_instance_from_subprocess,
                          args=(device_info, child_conn),
                          daemon=True)
        try:
            process.start()
            sila_device = parent_conn.recv()
            process.join()
        finally:
            process.close()
        return sila_device

    def get_features(self, uuid: UUID) -> List[Feature]:
        """Get the description of supported features of the specified device
        Args:
            uuid (uuid.UUID): The unique id of the device
        """
        device_info = self.get_device_info(uuid)
        parent_conn, child_conn = Pipe()
        process = Process(target=_get_device_features_from_subprocess,
                          args=(device_info, child_conn),daemon=True)
        features = None
        try:
            process.start()
            features = parent_conn.recv()
            process.join()
        finally:
            process.close()
            print('get_features process finished')
        return features

    def call_feature_command(self, device: UUID, feature: str, command: str,
                             params: Dict[str, any]):
        device_info = self.get_device_info(device)
        parent_conn, child_conn = Pipe()
        process = Process(target=_call_feature_command_from_subprocess,
                          args=(device_info, feature, command, params,
                                child_conn),daemon=True)
        result = None
        try:
            process.start()
            result = parent_conn.recv()
            process.join()
        finally:
            process.close()
            print('call_feature_command process finished')
        return result

    def get_feature_property(self, device: UUID, qualified_feature_identifier: str, prop: str):
        device_info = self.get_device_info(device)
        parent_conn, child_conn = Pipe()
        process = Process(target=_get_feature_property_from_subprocess,
                          args=(device_info, qualified_feature_identifier, prop, child_conn),daemon=True)
        result = None
        try:
            process.start()
            result = parent_conn.recv()
            process.join()
        finally:
            process.close()
            print('get_feature_property process finished')
        return result

    def add_features_for_data_handler(self, uuid: UUID):
        """Add the features of the device (specified by uuid) to the database
        Args:
            server_uuid: The uuid of the device for which to add the features to the database
        """
        sila_device = self.get_device_instance(uuid)

        sila_device.connect()
        features = self.get_features(uuid)
        conn = get_database_connection()
        with conn:
            with conn.cursor() as cursor:
                for i, feature in enumerate(features):
                    print(f'id_name: #{i}:', feature.identifier, feature.display_name)
                    try:
                        dynamic_feature = sila_device.getClient()._features[
                            feature.identifier]
                    except:
                        dynamic_feature = sila_device.getClient()._features[
                            feature.originator + '/' + feature.category + '/' + feature.identifier + '/v' +
                            str(feature.feature_version_major)]
                    cursor.execute(
                        'insert into features_for_data_handler values (default,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) returning id',
                        [
                            feature.identifier, feature.display_name,
                            feature.description, feature.sila2_version,
                            feature.originator,  feature.category,
                            feature.maturity_level, feature.locale,
                            feature.feature_version,
                            feature.feature_version_major,
                            feature.feature_version_minor,
                            str(uuid),
                            ACTIVE,
                            META
                        ])
                    feature_id = cursor.fetchone()[0]
                    for command in feature.commands:
                        dynamic_command = dynamic_feature.commands[
                            command.identifier]
                        cursor.execute(
                            'insert into commands_for_data_handler values (default,%s,%s,%s,%s,%s,%s,%s,%s,%s)' \
                            'returning id',
                            [command.identifier, command.display_name, command.description, command.observable, INTERVAL,
                             META_INTERVAL, ACTIVE, META, feature_id])
                        command_id = cursor.fetchone()[0]
                        for parameter in command.parameters:
                            if parameter.data_type != 'Void':
                                data_parameters = dynamic_command.parameters
                                parameter_index = list(
                                    data_parameters.fields.keys()).index(
                                        parameter.identifier)
                                parameter.data_type = list(data_parameters.paths.keys())[parameter_index].split('/', 1)[1]
                            cursor.execute(
                                'insert into parameters_for_data_handler values' \
                                '(default,%s,%s,%s,%s,%s,%s,%s,%s)',
                                [parameter.identifier, parameter.display_name, parameter.description, parameter.data_type, None,
                                 "parameter", "command", command_id])
                        for response in command.responses:
                            if response.data_type != 'Void':
                                data_responses = dynamic_command.responses
                                response_index = list(
                                    data_responses.fields.keys()).index(
                                        response.identifier)
                                response.data_type = list(data_responses.paths.keys())[response_index].split('/', 1)[1]
                            cursor.execute(
                                'insert into responses_for_data_handler values' \
                                '(default,%s,%s,%s,%s,%s,%s,%s,%s)',
                                [response.identifier, response.display_name, response.description, response.data_type, None,
                                 "response", "command", command_id])
                        for intermediate in command.intermediates:
                            data_intermediates = dynamic_command.intermediate_responses
                            intermediate_index = list(
                                data_intermediates.fields.keys()).index(
                                    intermediate.identifier)
                            intermediate.data_type = list(
                                data_intermediates.paths.keys(
                                ))[intermediate_index].split('/', 1)[1]
                            cursor.execute(
                                'insert into intermediate_response_for_data_handler values' \
                                '(default,%s,%s,%s,%s,%s,%s,%s,%s)',
                                [intermediate.identifier, intermediate.display_name, intermediate.description,
                                 intermediate.data_type, None, "intermediate", "command", command_id])
                        for defined_execution_error in command.defined_execution_errors:
                            cursor.execute(
                                'insert into defined_execution_errors values (default,%s,%s,%s)',
                                [
                                    defined_execution_error, "command",
                                    command_id
                                ])
                    for property in feature.properties:
                        dynamic_property = dynamic_feature.properties[
                            property.identifier]
                        cursor.execute(
                            'insert into properties_for_data_handler values (default,%s,%s,%s,%s,%s,%s,%s,%s,%s)' \
                            'returning id',
                            [property.identifier, property.display_name, property.description, property.observable, INTERVAL,
                             META_INTERVAL, ACTIVE, META, feature_id])
                        property_id = cursor.fetchone()[0]
                        response = property.response
                        data_responses = dynamic_property.responses
                        response_index = list(
                            data_responses.fields.keys()).index(
                                response.identifier)
                        response.data_type = list(
                            data_responses.paths.keys())[response_index].split(
                                '/', 1)[1]
                        cursor.execute(
                            'insert into responses_for_data_handler values' \
                            '(default,%s,%s,%s,%s,%s,%s,%s,%s)',
                            [response.identifier, response.display_name, response.description, response.data_type, None,
                             "response", "property", property_id])
                        for defined_execution_error in property.defined_execution_errors:
                            cursor.execute(
                                'insert into defined_execution_errors values (default,%s,%s,%s)',
                                [
                                    defined_execution_error, "property",
                                    property_id
                                ])
        release_database_connection(conn)

    def delete_features(self, uuid: UUID):
        """Delete the features of the device (specified by uuid) from the database
        Args:
            uuid: The uuid of the device for which to delete the features from the database
        """
        conn = get_database_connection()
        with conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    'delete from features_for_data_handler where device = %s returning id',
                    [uuid])
                feature_ids = [feature_tuple[0] for feature_tuple in cursor.fetchall()]
        release_database_connection(conn)
        self.delete_commands(feature_ids)
        self.delete_properties(feature_ids)

    def delete_commands(self, feature_ids: List[int]):
        """Delete the commands of the specified features from the database
        Args:
            feature_ids: The list of feature ids for which to delete the commands
        """
        # Check if any feature ids have been passed
        if len(feature_ids) == 0:
            return
        # Convert to string of comma separated ids
        feature_ids = ','.join(str(feature_id) for feature_id in feature_ids)
        conn = get_database_connection()
        with conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    'delete from commands_for_data_handler where feature in ({feature_ids}) returning id'.format(
                        feature_ids=feature_ids))
                command_ids = [command_tuple[0] for command_tuple in cursor.fetchall()]
        release_database_connection(conn)
        self.delete_command_subelements(command_ids)

    def delete_command_subelements(self, command_ids: List[int]):
        """Delete the parameters, responses, intermediates and defined execution errors of the specified commands from
        the database
        Args:
            command_ids: The list of command ids for which to delete the subelements
        """
        # Check if any command ids have been passed
        if len(command_ids) == 0:
            return
        # Convert to string of comma separated ids
        command_ids = ','.join(str(command_id) for command_id in command_ids)
        conn = get_database_connection()
        with conn:
            with conn.cursor() as cursor:
                # Delete parameters, responses and intermediates
                cursor.execute(
                    'delete from parameters_for_data_handler where parent in ({command_ids}) and parent_type = %s'
                    .format(command_ids=command_ids),
                    ['command'])
                # Delete defined execution errors
                cursor.execute(
                    'delete from defined_execution_errors where parent in ({command_ids}) and parent_type = %s'
                    .format(command_ids=command_ids),
                    ['command'])
        release_database_connection(conn)

    def delete_properties(self, feature_ids: List[int]):
        """Delete the properties of the specified features from the database
        Args:
            feature_ids: The list of feature ids for which to delete the properties
        """
        # Check if any feature ids have been passed
        if len(feature_ids) == 0:
            return
        # Convert to string of comma separated ids
        feature_ids = ','.join(str(feature_id) for feature_id in feature_ids)
        conn = get_database_connection()
        with conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    'delete from properties_for_data_handler where feature in ({feature_ids}) returning id'.format(
                        feature_ids=feature_ids))
                property_ids = [property_tuple[0] for property_tuple in cursor.fetchall()]
        release_database_connection(conn)
        self.delete_property_subelements(property_ids)

    def delete_property_subelements(self, property_ids: List[int]):
        """Delete the response and defined execution errors of the specified properties from the database
        Args:
            property_ids: The list of property ids for which to delete the subelements
        """
        # Check if any property ids have been passed
        if len(property_ids) == 0:
            return
        # Convert to string of comma separated ids
        property_ids = ','.join(str(property_id) for property_id in property_ids)
        conn = get_database_connection()
        with conn:
            with conn.cursor() as cursor:
                # Delete responses
                cursor.execute(
                    'delete from parameters_for_data_handler where parent in ({property_ids}) and parent_type = %s'
                    .format(property_ids=property_ids),
                    ['property'])
                # Delete defined execution errors
                cursor.execute(
                    'delete from defined_execution_errors where parent in ({property_ids}) and parent_type = %s'
                    .format(property_ids=property_ids),
                    ['property'])
        release_database_connection(conn)

    def get_features_for_data_handler(
            self, uuid: UUID) -> List[FeatureForDataHandler]:
        """Get the features of the device (specified by uuid) from the database
        Args:
            uuid: The uuid of the device for which to get the features from the database
        """
        features=[]
        conn = get_database_connection()
        with conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    'select id,identifier,display_name,description,sila2_version,originator,category,maturity_level,locale,feature_version,feature_version_minor,feature_version_major,activated,meta from features_for_data_handler where device=%s',
                    [str(uuid)])
                result = cursor.fetchall()
                features = [
                    FeatureForDataHandler(id=row[0],
                                          identifier=row[1],
                                          display_name=row[2],
                                          description=row[3],
                                          sila2_version=row[4],
                                          originator=row[5],
                                          category=row[6],
                                          maturity_level=row[7],
                                          locale=row[8],
                                          feature_version=row[9],
                                          feature_version_minor=row[10],
                                          feature_version_major=row[11],
                                          commands=[],
                                          properties=[],
                                          active=row[12],
                                          meta=row[13])
                    for row in result
                ]
        release_database_connection(conn)
        for feature in features:
            feature.commands = self.get_commands_for_feature_for_data_handler(
                feature.id)
            feature.properties = self.get_properties_for_feature_for_data_handler(
                feature.id)
        return features

    def get_commands_for_feature_for_data_handler(
            self, feature_id) -> List[CommandForDataHandler]:
        """Get the commands of the feature (specified by feature_id) from the database
        Args:
            feature_id: The id of the feature for which to get the commands from the database
        """
        commands=[]
        conn = get_database_connection()
        with conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    'select identifier,display_name,description,observable,id,polling_interval_non_meta,polling_interval_meta,activated,meta from commands_for_data_handler where feature=%s',
                    [str(feature_id)])
                result = cursor.fetchall()
                commands = [
                    CommandForDataHandler(identifier=row[0],
                                          display_name=row[1],
                                          description=row[2],
                                          observable=row[3],
                                          parameters=[],
                                          responses=[],
                                          intermediates=[],
                                          defined_execution_errors=[],
                                          id=row[4],
                                          polling_interval_non_meta=row[5],
                                          polling_interval_meta=row[6],
                                          active=row[7],
                                          meta=row[8]) for row in result
                ]
        release_database_connection(conn)
        for command in commands:
            command.parameters = self.get_parameters_for_command_for_data_handler(
                command.id)
            command.responses = self.get_responses_for_command_for_data_handler(
                command.id)
            command.intermediates = self.get_intermediate_responses_for_command_for_data_handler(
                command.id)
            command.defined_execution_errors = \
                self.get_defined_execution_errors_for_command_for_data_handler(command.id)
        return commands

    def get_parameters_for_command_for_data_handler(
            self, command_id) -> List[CommandParameterForDataHandler]:
        """Get the parameters of the command (specified by command_id) from the database
        Args:
            command_id: The id of the command for which to get the parameters from the database
        """
        parameters=[]
        conn = get_database_connection()
        with conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    'select data_type,identifier,display_name,description,id,value from parameters_for_data_handler where used_as=%s and parent_type=%s and parent=%s',
                    ['parameter', 'command',
                     str(command_id)])
                result = cursor.fetchall()
                parameters = [
                    CommandParameterForDataHandler(data_type=row[0],
                                                   identifier=row[1],
                                                   display_name=row[2],
                                                   description=row[3],
                                                   id=row[4],
                                                   value=row[5])
                    for row in result
                ]
        release_database_connection(conn)
        return parameters

    def get_responses_for_command_for_data_handler(
            self, command_id) -> List[CommandResponseForDataHandler]:
        """Get the parameters of the command (specified by command_id) from the database
        Args:
            command_id: The id of the command for which to get the parameters from the database
        """
        parameters=[]
        conn = get_database_connection()
        with conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    'select data_type,identifier,display_name,description,id,value from responses_for_data_handler where used_as=%s and parent_type=%s and parent=%s',
                    ['response', 'command',
                     str(command_id)])
                result = cursor.fetchall()
                responses = [
                    CommandResponseForDataHandler(data_type=row[0],
                                                  identifier=row[1],
                                                  display_name=row[2],
                                                  description=row[3],
                                                  id=row[4],
                                                  value=row[5])
                    for row in result
                ]
        release_database_connection(conn)
        return responses

    def get_intermediate_responses_for_command_for_data_handler(
            self, command_id) -> List[IntermediateCommandResponseForDataHandler]:
        """Get the parameters of the command (specified by command_id) from the database
        Args:
            command_id: The id of the command for which to get the parameters from the database
        """
        parameters=[]
        conn = get_database_connection()
        with conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    'select data_type,identifier,display_name,description,id,value from intermediate_responses_for_data_handler where used_as=%s and parent_type=%s and parent=%s',
                    ['intermediate', 'command',
                     str(command_id)])
                result = cursor.fetchall()
                intermediate_responses = [
                    IntermediateCommandResponseForDataHandler(data_type=row[0],
                                                              identifier=row[1],
                                                              display_name=row[2],
                                                              description=row[3],
                                                              id=row[4],
                                                              value=row[5])
                    for row in result
                ]
        release_database_connection(conn)
        return intermediate_responses

    def get_defined_execution_errors_for_command_for_data_handler(
            self, command_id) -> List[str]:
        """Get the defined execution errors of the command (specified by command_id) from the database
        Args:
            command_id: The id of the command for which to get the defined execution errors from the database
        """
        defined_execution_errors=[]
        conn = get_database_connection()
        with conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    'select defined_execution_error from defined_execution_errors where parent_type=%s and parent=%s',
                    ['command', str(command_id)])
                result = cursor.fetchall()
                defined_execution_errors = [row[0] for row in result]
        release_database_connection(conn)
        return defined_execution_errors

    def get_properties_for_feature_for_data_handler(
            self, feature_id) -> List[PropertyForDataHandler]:
        """Get the properties of the feature (specified by feature_id) from the database
        Args:
            feature_id: The id of the feature for which to get the properties from the database
        """
        properties=[]
        conn = get_database_connection()
        with conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    'select id,identifier,display_name,description,observable,polling_interval_non_meta,polling_interval_meta,activated,meta from properties_for_data_handler where feature=%s',
                    [str(feature_id)])
                result = cursor.fetchall()
                properties = [
                    PropertyForDataHandler(id=row[0],
                                           identifier=row[1],
                                           display_name=row[2],
                                           description=row[3],
                                           observable=row[4],
                                           response=None,
                                           defined_execution_errors=[],
                                           polling_interval_non_meta=row[5],
                                           polling_interval_meta=row[6],
                                           active=row[7],
                                           meta=row[8]) for row in result

                ]
                for property in properties:
                    property.response = self.get_response_for_property_for_data_handler(
                        property.id)
                    property.defined_execution_errors = \
                        self.get_defined_execution_errors_for_property_for_data_handler(property.id)

        release_database_connection(conn)
        return properties

    def get_response_for_property_for_data_handler(
            self, property_id) -> PropertyResponseForDataHandler:
        """Get the response of the property (specified by property_id) from the database
        Args:
            property_id: The id of the property for which to get the response from the database
        """
        response=None
        conn = get_database_connection()
        with conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    'select data_type,identifier,display_name,description,id,value from responses_for_data_handler where used_as=%s and parent_type=%s and parent=%s',
                    ['response', 'property',
                     str(property_id)])
                row = cursor.fetchone()
                response = PropertyResponseForDataHandler(data_type=row[0],
                                                          identifier=row[1],
                                                          display_name=row[2],
                                                          description=row[3],
                                                          id=row[4],
                                                          value=row[5])
        release_database_connection(conn)
        return response

    def get_defined_execution_errors_for_property_for_data_handler(
            self, property_id) -> List[str]:
        """Get the defined execution errors of the property (specified by property_id) from the database
        Args:
            property_id: The id of the property for which to get the defined execution errors from the database
        """
        defined_execution_errors=[]
        conn = get_database_connection()
        with conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    'select defined_execution_error from defined_execution_errors where parent_type=%s and parent=%s',
                    ['property', str(property_id)])
                result = cursor.fetchall()
                defined_execution_errors = [row[0] for row in result]
        release_database_connection(conn)
        return defined_execution_errors

    def get_database_info_list(self) -> List[DatabaseInfo]:
        """Returns a list of database information from the database"""
        info_list=[]
        conn = get_database_connection()
        with conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    'select id,name,address,port, username, password from databases'
                )
                result = cursor.fetchall()
                info_list=[
                    DatabaseInfo(row[0], row[1], row[2], row[3], row[4], row[5]) for row in result
                ]
        release_database_connection(conn)
        return info_list

    def get_database_info(self, id: int) -> DatabaseInfo:
        """Returns the specified database info
        Args:
            id: The id of the database
        Returns:
            DatabaseInfo: An instance of Database info
        """
        info=None
        conn = get_database_connection()
        with conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    'select id,name,address,port, username, password from databases ' \
                    'where id=%s',
                    [str(id)])
                database = cursor.fetchone()
                info = DatabaseInfo(database[0], database[1], database[2], database[3], database[4], database[5])
        release_database_connection(conn)
        return info

    def get_database_status(self, id: int) -> DatabaseStatus:
        """Get the current status of the specified database
        Args:
            id: The id of the database
        """
        database_info = self.get_database_info(id)
        parent_conn, child_conn = Pipe()
        process = Process(target=_get_database_status_from_subprocess,
                          args=(database_info, child_conn))
        device_status = None
        # Todo: Add proper implementation here!
        try:
            process.start()
            database_status = parent_conn.recv()
            process.join()
            # database_status = DatabaseStatus(True, 'Not implemented yet!')
        finally:
            process.close()
            print('get_status process finished')
        return database_status

    def add_database(self, name: str, address: str, port: int, username: str, password: str):
        """Add a new database to the database
        Args:
            name: The name of the new database that should be added to the database
            address: The IP address of the new database that should be added to the database
            port: The port of the new database that should be added to the database
            username: The username that is needed as login credential for the new database
            password: The password for the new database login
        """
        conn = get_database_connection()
        with conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    'insert into databases values (default,%s,%s,%s,%s,%s)',
                    [name, address, port, username, password])
        release_database_connection(conn)

    def set_database(self, id: int, name: str, address: str, port: int, username: str, password: str):
        """Updates a database in the database
        Args:
            id: The id of the database to update
            name: The new name to set to the database
            address: The new IP address to set to the database
            port: The new port to set to the database
            username: The username to set to the new database
            password: The password to set to the new database
        """
        conn = get_database_connection()
        with conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    'update databases set name=%s, address=%s, port=%s, username=%s, password=%s ' \
                    'where id=%s',
                    [
                        name, address, port, username, password, id
                    ])
        release_database_connection(conn)

    def delete_database(self, id: int):
        """Delete a database from the database
        Args:
            id: The id of the database
        """
        conn = get_database_connection()
        with conn:
            with conn.cursor() as cursor:
                cursor.execute('delete from databases where id=%s',
                               [id])
                # Unlink database for devices that have this database
                cursor.execute('update devices set databaseID = %s where databaseID=%s',
                               [None, id])
        release_database_connection(conn)


    def link_database(self, device_uuid: UUID, database_id: int):
        """Link a device to a database
        Args:
            device_uuid: The UUID of the device to link
            database_id: The id of the database to link
        """
        conn = get_database_connection()
        with conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    'update devices set databaseID = %s where uuid = %s',
                    [database_id, device_uuid])
        release_database_connection(conn)

    def unlink_database(self, device_uuid: UUID):
        """Removes the database link of the specified device
        Args:
            device_uuid: The UUID of the device for which to remove the database link
        """
        conn = get_database_connection()
        with conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    'update devices set databaseID = %s where uuid = %s',
                    [None, device_uuid])
        release_database_connection(conn)

    def set_device_attributes_for_data_handler(self, device_uuid: UUID, active: bool):
        """Set the 'active' attribute of the specified device and its features, commands and properties
        to the specified value
        Args:
            device_uuid: The UUID of the device
            active: The new value of the 'active' attribute
        """
        conn = get_database_connection()
        with conn:
            with conn.cursor() as cursor:
                # Update the device
                cursor.execute(
                    'update devices set activated = %s where uuid = %s',
                    [active, device_uuid])
                # Update the features of the device and get the ids of the features
                cursor.execute(
                    'update features_for_data_handler set activated = %s where device = %s returning id',
                    [active, device_uuid])
                feature_ids = cursor.fetchall()
                # Convert to string of comma separated ids
                feature_ids = ','.join(str(x[0]) for x in feature_ids)
                # Update the commands and properties
                cursor.execute(
                    'update commands_for_data_handler set activated = %s where feature in ({feature_ids})'.format(
                        feature_ids=feature_ids),
                    [active])
                cursor.execute(
                    'update properties_for_data_handler set activated = %s where feature in ({feature_ids})'.format(
                        feature_ids=feature_ids),
                    [active])
        release_database_connection(conn)

    def set_feature_attributes_for_data_handler(self, device_uuid: UUID, feature_id: str, active: bool, meta: bool):
        """Set the 'active' and 'meta' attributes of the specified feature and its commands and properties
        to the specified values
        Args:
            device_uuid: The uuid of the device containing the feature
            feature_id: The id of the feature
            active: The new value of the 'active' attribute
            meta: The new value of the 'meta' attribute
        """
        conn = get_database_connection()
        with conn:
            with conn.cursor() as cursor:
                # Update the feature
                cursor.execute(
                    'update features_for_data_handler set activated = %s, meta = %s where id = %s',
                    [active, meta, feature_id])
                # Update the commands and properties
                cursor.execute(
                    'update commands_for_data_handler set activated = %s, meta = %s where feature = %s',
                    [active, meta, feature_id])
                cursor.execute(
                    'update properties_for_data_handler set activated = %s, meta = %s where feature = %s',
                    [active, meta, feature_id])
                # Retrieve the 'active' attribute of the features of the device
                cursor.execute(
                    'select activated from features_for_data_handler where device = %s',
                    [device_uuid])
                device_features_active = cursor.fetchall()
                # Update the device 'active' attribute to be consistent with the new feature 'active' attribute:
                # - if all features of a device are active, then the device should be active as well
                # - otherwise the device should NOT be active
                device_active = True
                for feature_active in device_features_active:
                    device_active = device_active and feature_active[0]
                cursor.execute(
                    'update devices set activated = %s where uuid = %s',
                    [device_active, device_uuid])
        release_database_connection(conn)

    def set_command_attributes_for_data_handler(self, device_uuid: UUID, feature_id: str, command_id: str, active: bool,
                                                meta: bool, polling_interval_non_meta: int, polling_interval_meta: int,
                                                parameters):
        """Set the attributes of the specified command to the specified values
        Args:
            device_uuid: The uuid of the device
            feature_id: The id of the feature
            command_id: the id of the command
            active: The new value of the 'active' attribute
            meta: The new value of the 'meta' attribute
            polling_interval_non_meta: The new value of the 'polling_interval_non_meta' attribute
            polling_interval_meta: The new value of the 'polling_interval_meta' attribute
            parameters: The new values of the parameters of the command
        """
        # Check if polling_interval_non_meta values are specified: if not, use defaults
        if polling_interval_non_meta is None:
            polling_interval_non_meta = INTERVAL
        if polling_interval_meta is None:
            polling_interval_meta = META_INTERVAL
        conn = get_database_connection()
        with conn:
            with conn.cursor() as cursor:
                # Update parameter values
                for parameter in parameters:
                    cursor.execute(
                        'update parameters_for_data_handler set value = %s where identifier = %s and used_as = %s and parent_type = %s and parent = %s',
                        [parameter.value, parameter.name, 'parameter', 'command', command_id])
                # Update the command
                cursor.execute(
                    'update commands_for_data_handler set activated = %s, meta = %s, polling_interval_non_meta = %s, polling_interval_meta = %s where id = %s',
                    [active, meta, polling_interval_non_meta, polling_interval_meta, command_id])
                # Retrieve the 'active' and 'meta' attributes of the commands and properties of the feature
                cursor.execute(
                    'select activated, meta from commands_for_data_handler where feature = %s',
                    [feature_id])
                feature_commands_attributes = cursor.fetchall()
                cursor.execute(
                    'select activated, meta from properties_for_data_handler where feature = %s',
                    [feature_id])
                feature_properties_attributes = cursor.fetchall()
                # Update the feature 'active' and 'meta' attributes to be consistent with
                # the new command 'active' and 'meta' attributes:
                # - if all commands and properties of a feature are active, then the feature should be active as well
                # - otherwise the feature should NOT be active
                # And similar for the 'meta' attribute
                feature_active = True
                feature_meta = True
                for command_attributes in feature_commands_attributes:
                    feature_active = feature_active and command_attributes[0]
                    feature_meta = feature_meta and command_attributes[1]
                for property_attributes in feature_properties_attributes:
                    feature_active = feature_active and property_attributes[0]
                    feature_meta = feature_meta and property_attributes[1]
                cursor.execute(
                    'update features_for_data_handler set activated = %s, meta = %s where id = %s',
                    [feature_active, feature_meta, feature_id])
                # Retrieve the 'active' attribute of the features of the device
                cursor.execute(
                    'select activated from features_for_data_handler where device = %s',
                    [device_uuid])
                device_features_active = cursor.fetchall()
                # Update the device 'active' attribute to be consistent with the new feature 'active' attribute:
                # - if all features of a device are active, then the device should be active as well
                # - otherwise the device should NOT be active
                device_active = True
                for feature_active in device_features_active:
                    device_active = device_active and feature_active[0]
                cursor.execute(
                    'update devices set activated = %s where uuid = %s',
                    [device_active, device_uuid])
        release_database_connection(conn)

    def set_property_attributes_for_data_handler(self, device_uuid: UUID, feature_id: str, property_id: str,
                                                 active: bool, meta: bool, polling_interval_non_meta: int,
                                                 polling_interval_meta: int):
        """Set the attributes of the specified property to the specified values
        Args:
            device_uuid: The uuid of the device
            feature_id: The id of the feature
            property_id: the id of the property
            active: The new value of the 'active' attribute
            meta: The new value of the 'meta' attribute
            polling_interval_non_meta: The new value of the 'polling_interval_non_meta' attribute
            polling_interval_meta: The new value of the 'polling_interval_meta' attribute
        """
        # Check if polling_interval_non_meta values are specified: if not, use defaults
        if polling_interval_non_meta is None:
            polling_interval_non_meta = INTERVAL
        if polling_interval_meta is None:
            polling_interval_meta = META_INTERVAL
        conn = get_database_connection()
        with conn:
            with conn.cursor() as cursor:
                # Update the property
                cursor.execute(
                    'update properties_for_data_handler set activated = %s, meta = %s, polling_interval_non_meta = %s, polling_interval_meta = %s where id = %s',
                    [active, meta, polling_interval_non_meta, polling_interval_meta, property_id])
                # Retrieve the 'active' and 'meta' attributes of the commands and properties of the feature
                cursor.execute(
                    'select activated, meta from commands_for_data_handler where feature = %s',
                    [feature_id])
                feature_commands_attributes = cursor.fetchall()
                cursor.execute(
                    'select activated, meta from properties_for_data_handler where feature = %s',
                    [feature_id])
                feature_properties_attributes = cursor.fetchall()
                # Update the feature 'active' and 'meta' attributes to be consistent with
                # the new property 'active' and 'meta' attributes:
                # - if all commands and properties of a feature are active, then the feature should be active as well
                # - otherwise the feature should NOT be active
                # And similar for the 'meta' attribute
                feature_active = True
                feature_meta = True
                for command_attributes in feature_commands_attributes:
                    feature_active = feature_active and command_attributes[0]
                    feature_meta = feature_meta and command_attributes[1]
                for property_attributes in feature_properties_attributes:
                    feature_active = feature_active and property_attributes[0]
                    feature_meta = feature_meta and property_attributes[1]
                cursor.execute(
                    'update features_for_data_handler set activated = %s, meta = %s where id = %s',
                    [feature_active, feature_meta, feature_id])
                # Retrieve the 'active' attribute of the features of the device
                cursor.execute(
                    'select activated from features_for_data_handler where device = %s',
                    [device_uuid])
                device_features_active = cursor.fetchall()
                # Update the device 'active' attribute to be consistent with the new feature 'active' attribute:
                # - if all features of a device are active, then the device should be active as well
                # - otherwise the device should NOT be active
                device_active = True
                for feature_active in device_features_active:
                    device_active = device_active and feature_active[0]
                cursor.execute(
                    'update devices set activated = %s where uuid = %s',
                    [device_active, device_uuid])
        release_database_connection(conn)

    def discover_sila_devices(self):
        """Triggers the sila autodiscovery
        Returns:
            The list of discovered devices
        """
        return find()

    def get_log(self,
                from_date: int = 0,
                to_date: int = datetime.now().timestamp(),
                exclude=None):
        """Get log entries from database
        Args:
            start (datetime): The first date
            end (datetime): The last date
            exclude: A dictionary containing the log levels that
            should be excluded
        Returns:
            Log entries
        """
        exclude_string = ''

        if exclude is not None:
            if exclude['info']:
                exclude_string += f'type != {LogLevel.INFO} '
            if exclude['warning']:
                seperator = 'and ' if exclude['info'] else ''
                exclude_string += seperator + f'type != {LogLevel.WARNING} '
            if exclude['critical']:
                seperator = 'and ' if exclude['info'] or exclude[
                    'warning'] else ''
                exclude_string += seperator + f'type != {LogLevel.CRITICAL} '
            if exclude['error']:
                seperator = 'and ' if exclude['info'] or exclude[
                    'warning'] or exclude['critical'] else ''
                exclude_string += seperator + f'type != {LogLevel.ERROR} '

            exclude_string += 'and ' if exclude['info'] or exclude[
                'warning'] or exclude['critical'] or exclude['error'] else ''
            print(exclude_string)

        log=[]
        conn = get_database_connection()
        with conn:
            with conn.cursor() as cursor:
                cursor.execute(f'select type,device,time,message from log '\
                f'where '\
                f'{exclude_string} '\
                f'time>=%s and time<=%s order by time desc',
                [from_date, to_date])
                log = [{
                    'type': row[0],
                    'device': row[1],
                    'time': row[2],
                    'message': row[3]
                } for row in cursor]
        release_database_connection(conn)
        return log

    def get_booking_entry(self, id: int):
        return get_booking_entry(id)

    def get_device_bookings(self, device: UUID, start: int,
                            stop: int) -> List[BookingInfo]:
        return get_device_booking_info(device, start, stop)

    def get_all_bookings(self, start: int, stop: int) -> List[BookingInfo]:
        return get_booking_info(start, stop)

    def get_device_bookings_with_name(self, device: UUID, start: int,
                                      stop: int) -> List[BookingInfoWithNames]:
        return get_device_booking_info_with_names(device, start, stop)

    def get_all_bookings_with_name(self, start: int,
                                   stop: int) -> List[BookingInfoWithNames]:
        return get_booking_info_with_names(start, stop)

    def book_device(self, name: str, user: int, device: UUID, start: int,
                    stop: int) -> int:
        return book(BookingInfo(-1, name, start, stop, user, device))

    def delete_booking_entry(self, id: int):
        if id_is_valid(id):
            delete_booking_entry(id)

    def get_all_experiments(self) -> experiment.Experiment:
        return experiment.get_all_experiments()

    def get_user_experiments(self, user: int) -> experiment.Experiment:
        return experiment.get_user_experiments(user)

    def create_experiment(self, name: str, start: int, end: int, user: int,
                          devices: List[UUID], script: int) -> int:
        return experiment.create_experiment(name, start, end, user, devices,
                                            script)

    def edit_experiment(self, experimentID: int, name: str, start: int, end: int, user: int,
                          devices: List[UUID], script: int) -> int:
        return experiment.edit_experiment(experimentID, name, start, end, user, devices,
                                            script)

    def delete_experiment(self, experimentID: int):
        return experiment.delete_experiment(experimentID)

    def get_user_scripts(self, user: int) -> List[script.Script]:
        return script.get_user_scripts(user)

    def get_user_scripts_info(self, user: int) -> List[script.ScriptInfo]:
        return script.get_user_scripts_info(user)

    def get_user_script(self, script_id: int) -> script.Script:
        return script.get_user_script(script_id)

    def get_user_script_info(self, script_id: int) -> script.ScriptInfo:
        return script.get_user_script_info(script_id)

    def create_user_script(self, name: str, fileName: str, user: int,
                           data: str) -> int:
        return script.create_user_script(name, fileName, user, data)

    def delete_user_script(self, script_id: int):
        script.delete_user_script(script_id)

    def set_user_script_info(self, script_id: int, name: str, file_name: str,
                             user_id: int):
        script.set_user_script_info(script_id, name, file_name, user_id)

    def set_user_script(self, script_id: int, name: str, file_name: str,
                        user_id: int, data: str):
        script.set_user_script(script_id, name, file_name, user_id, data)
