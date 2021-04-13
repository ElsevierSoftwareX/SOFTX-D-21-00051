import logging
import sys

from typing import Optional
from typing import Dict, List, Any

import os
import grpc

from sila2lib.sila_client import SiLA2Client

from sila2lib.framework.std_features import SiLAService_pb2, SimulationController_pb2
from sila2lib.framework import SiLAFramework_pb2 as silaFW_pb2

from sila2lib.proto_builder.dynamic_feature import DynamicFeature
from sila2lib.proto_builder.data.data_base import DataBase

from source.device_manager.data_directories import TEMP_DIRECTORY
from uuid import UUID
import filelock
import shutil
from dataclasses import dataclass


def get_sila_device_directory(uuid: UUID) -> str:
    """
    Get the applications temp files directory path

    :param uuid: The uuid of the SiLA server
    :type uuid: str
    """
    return os.path.join(TEMP_DIRECTORY, 'Sila', str(uuid))


def get_sila_device_lock_file(uuid: UUID) -> str:
    """
    Get the lock file of the files

    :param uuid: The uuid of the SiLA server
    :type uuid: str
    """
    return os.path.join(TEMP_DIRECTORY, 'Sila', f'{str(uuid)}.lock')


def delete_dynamic_client(uuid: UUID):
    """
    Delete the files generated by the dynamic client from the temporary files folder

    :param uuid: The uuid of the SiLA server
    :type uuid: str
    """
    directory = get_sila_device_directory(uuid)
    lock_file = get_sila_device_lock_file(uuid)
    if os.path.exists(directory):
        shutil.rmtree(directory)
    if os.path.exists(lock_file):
        os.remove(lock_file)

@dataclass
class FunctionParameterDescription:
    identifier: str
    input_data_type: List[str]
    input_data_path: List[str]
    output_data_type: List[str]
    output_data_path: List[str]

    def __init__(self, identifier):
        self.identifier = identifier
        self.input_data_type = []
        self.input_data_path = []
        self.output_data_type = []
        self.output_data_path = []


class DynamicSiLA2Client(SiLA2Client):
    """ The dynamic client class """
    #: Storage for all features read from the server
    _features: Dict[str, DynamicFeature]

    #: Path where the general information about the server is stored
    data_storage: str

    def __init__(self,
                 name: str,
                 description: str = "",
                 server_name: Optional[str] = None,
                 client_uuid: Optional[str] = None,
                 version: str = "0.0",
                 vendor_url: str = "",
                 server_hostname: str = "localhost",
                 server_ip: str = '127.0.0.1',
                 server_port: int = 50051):
        super().__init__(name, description, server_name, client_uuid, version,
                         vendor_url, server_hostname, server_ip, server_port)
        # get the servers UUID
        response = self.SiLAService_stub.Get_ServerUUID(
            SiLAService_pb2.Get_ServerUUID_Parameters())
        self.server_uuid = response.ServerUUID.value

        response = self.SiLAService_stub.Get_ServerName(
            SiLAService_pb2.Get_ServerName_Parameters())
        self.server_name = response.ServerName.value

        # derive and prepare the storage directory
        self.data_storage = get_sila_device_directory(self.server_uuid)

        self.lock_file_name = get_sila_device_lock_file(self.server_uuid)
        os.makedirs(os.path.join(TEMP_DIRECTORY, 'Sila'), exist_ok=True)
        #self.first_time = False

        # prepare object variables
        self._features = {}

    def generate_files(self):
        """ Creates the Feature Definition (FDL) xml-files, proto, pb2 and pb2_grpc files in the temporary files directory """
        try:
            os.makedirs(self.data_storage, exist_ok=True)
            with open(os.path.join(self.data_storage, '.server_name'),
                      'w',
                      encoding='utf-8') as file:
                file.write(self.server_name)
            # we need to get all features
            response = self.SiLAService_stub.Get_ImplementedFeatures(
                SiLAService_pb2.Get_ImplementedFeatures_Parameters())
            feature_list_path = os.path.join(self.data_storage, 'features.txt')

            with open(feature_list_path, 'w',
                      encoding='utf-8') as feature_list_file:
                for feature_response in response.ImplementedFeatures:
                    qualified_feature_identifier = feature_response.value
                    # if we find a feature for which is already implemented ignore it
                    # in the case of the device manager, we actually want the standard features to be displayed as well,
                    # so we deactivate this if-clause!
                    # if feature_id in ['SiLAService', 'SimulationController']:
                    #     logging.debug(
                    #         'Implemented standard feature {feature} found, '
                    #         'skipping dynamic handling for this feature'.
                    #         format(feature=feature_id))
                    #     continue

                    # read the feature definition
                    logging.info('Found implemented feature {feature}'.format(
                        feature=qualified_feature_identifier))
                    try:
                        response = self.SiLAService_stub.GetFeatureDefinition(
                            SiLAService_pb2.GetFeatureDefinition_Parameters(
                                QualifiedFeatureIdentifier=silaFW_pb2.String(
                                    value=str.encode(qualified_feature_identifier))))
                        fdl_string = response.FeatureDefinition.value
                    except grpc.RpcError:
                        logging.error(
                            'Could not load feature definition of {feature}'.
                            format(feature=qualified_feature_identifier))
                        continue

                    feature_list_file.write(f'{qualified_feature_identifier}\n')

                    # write the corresponding fdl file locally
                    if '/' in qualified_feature_identifier:
                        # For qualified features

                        originator = qualified_feature_identifier.split('/')[0].strip()  # e.g. org.silastandard
                        remainder = qualified_feature_identifier.split(originator)[1].strip()
                        fdl_filepath = os.path.join(self.data_storage, originator)
                        os.makedirs(fdl_filepath, exist_ok=True)
                        if '/' in remainder:
                            category = qualified_feature_identifier.split('/')[1].strip()  # e.g core
                            remainder = qualified_feature_identifier.split(category)[1].strip()
                            fdl_filepath = os.path.join(fdl_filepath, category)
                            os.makedirs(fdl_filepath, exist_ok=True)
                            if '/' in remainder:
                                feature_identifier = qualified_feature_identifier.split('/')[2].strip()  # e.g core
                                major_feature_version = qualified_feature_identifier.split(
                                    feature_identifier + '/')[1].strip()
                                os.makedirs(os.path.join(fdl_filepath, feature_identifier), exist_ok=True)
                                fdl_filepath = os.path.join(fdl_filepath, feature_identifier, major_feature_version)
                                os.makedirs(fdl_filepath, exist_ok=True)

                            else:
                                feature_identifier = qualified_feature_identifier.split('/')[2].strip()  # e.g core
                                os.makedirs(os.path.join(self.data_storage,
                                                         originator,
                                                         category,
                                                         feature_identifier,
                                                         major_feature_version
                                                         ),
                                            exist_ok=True
                                            )
                                fdl_filepath = os.path.join(fdl_filepath, feature_identifier)

                        fdl_filename = os.path.join(
                            fdl_filepath,
                            f'{feature_identifier}.sila.xml'
                        )
                        with open(fdl_filename, 'w', encoding='utf-8') as fdl_file:
                            fdl_file.write(fdl_string)
                    else:
                        # For non-qualified features
                        feature_identifier = qualified_feature_identifier
                        fdl_filename = os.path.join(
                            self.data_storage,
                            f'{feature_identifier}.sila.xml')
                        with open(fdl_filename, 'w', encoding='utf-8') as fdl_file:
                            fdl_file.write(fdl_string)

                    # fdl_filename = os.path.join(
                    #    self.data_storage,
                    #    '{feature}.sila.xml'.format(feature=feature_id))
                    # with open(fdl_filename, 'w', encoding='utf-8') as fdl_file:
                    #    fdl_file.write(fdl_string)

        except Exception:
            logging.error("Error during dynamic client file creation")
            shutil.rmtree(self.data_storage)
            raise

    def run(self):
        with filelock.FileLock(self.lock_file_name, timeout=10):
            print(f'{self.lock_file_name} acquired')
            if not os.path.exists(self.data_storage):
                logging.info(f'creating {self.data_storage}')
                self.generate_files()

            feature_list_path = os.path.join(self.data_storage, 'features.txt')
            with open(feature_list_path, 'r') as feature_list_file:
                for qualified_feature_identifier in feature_list_file.readlines():
                    # generate the dynamic handler
                    if '/' in qualified_feature_identifier:
                        # For qualified features:
                        originator, category, feature_identifier, major_feature_version = \
                            qualified_feature_identifier.split('/')
                        fdl_filename = os.path.join(self.data_storage,
                                                    originator.strip(),
                                                    category.strip(),
                                                    feature_identifier.strip(),
                                                    major_feature_version.strip(),
                                                    f'{feature_identifier.strip()}.sila.xml')
                        self._features[qualified_feature_identifier.strip('\n')] = \
                            DynamicFeature(fdl_file=fdl_filename, channel=self.channel)
                        del sys.modules[feature_identifier.strip() + '_pb2' ]
                        del sys.modules[feature_identifier.strip() + '_pb2_grpc']
                    else:
                        # For unqualified features:
                        fdl_filename = os.path.join(
                            self.data_storage,
                            f'{qualified_feature_identifier.strip()}.sila.xml'
                        )
                        self._features[qualified_feature_identifier.strip('\n')] = \
                            DynamicFeature(fdl_file=fdl_filename, channel=self.channel)

                        del sys.modules[qualified_feature_identifier.rstrip('\n') + '_pb2']
                        del sys.modules[qualified_feature_identifier.rstrip('\n') + '_pb2_grpc']

                    # del sys.modules[feature_id.rstrip('\n') + '_pb2']
                    # del sys.modules[feature_id.rstrip('\n') + '_pb2_grpc']
                    sys.path.remove(os.path.dirname(fdl_filename))

    def stop(self, force: bool = False):
        # nothing to do I guess
        pass

    def list_features(self) -> List[str]:
        return list(self._list_names(self._features))

    def get_feature_path(self, feature_id) -> str:
        return os.path.join(self.data_storage,
                            f'{feature_id.strip()}.sila.xml')

    def list_command_names(self, feature_id: str) -> List[str]:
        return list(self._list_names(self._features[feature_id].commands))

    def list_commands(self, feature_id: str):
        _commands = self.list_command_names(feature_id)

        # initialise the return list as empty
        function_parameter_descriptions: List[
            FunctionParameterDescription] = []

        for _command in _commands:
            func_par_desc = FunctionParameterDescription(_command)

            for parameter in self._features[feature_id].commands[
                    _command].parameters.paths:
                func_par_desc.input_data_path.append(parameter)
                func_par_desc.input_data_type.append(
                    parameter.rsplit(DataBase.path_separator, 1)[-1])
            for response in self._features[feature_id].commands[
                    _command].responses.paths:
                func_par_desc.output_data_path.append(response)
                func_par_desc.output_data_type.append(
                    response.rsplit(DataBase.path_separator, 1)[-1])

            function_parameter_descriptions.append(func_par_desc)

        return function_parameter_descriptions

    def count_commands(self, feature_id: str) -> int:
        return len(self._features[feature_id].commands)

    def list_property_names(self, feature_id: str) -> List[str]:
        return list(self._list_names(self._features[feature_id].properties))

    def list_properties(self, feature_id: str):
        _properties = self.list_property_names(feature_id)

        # initialise the return list as empty
        function_parameter_descriptions: List[
            FunctionParameterDescription] = []

        for _property in _properties:
            func_par_desc = FunctionParameterDescription(_property)

            for response in self._features[feature_id].properties[
                    _property].responses.paths:
                func_par_desc.output_data_path.append(response)
                func_par_desc.output_data_type.append(
                    response.rsplit(DataBase.path_separator, 1)[-1])

            function_parameter_descriptions.append(func_par_desc)

        return function_parameter_descriptions

    def count_properties(self, feature_id: str) -> int:
        return len(self._features[feature_id].properties)

    def call_command(self, feature_id: str, command_id: str,
                     parameters: Dict[str, Any]) -> Dict[str, Any]:
        # for key in self._features.keys():
        #    if feature_id in key:
        #        print('key', key)
        #        break
        try:
            #    print(self._features[key].commands.keys())
            #    print(command_id)
            #    for command_key in self._features[key].commands.keys():
            #        if command_id in command_key:
            #            print('truesssss')
            #            break
            command_object = self._features[feature_id].commands[command_id]
        except KeyError:
            for key in self._features.keys():
                if feature_id in key:
                    #            print('featureid/key', feature_id, key)
                    feature_id = key
                    break
            #    print(f'Feature {feature_id} does not use a qualified identifier!')
            #    print(feature_id.split('/')[-2])
            #    unqualified_feature_id = feature_id.split('/')[-2]
            command_object = self._features[feature_id].commands[command_id]

        # set the parameters
        for parameter_path in parameters:
            # Ensure to except parameters that are implemented in the xml, but not the actual server implementation
            # if 'emptyparameter' in parameter_path:  # Intercept by parameter name
            if parameters[parameter_path] == '':  # Intercept by empty value
                break
            command_object.parameters.set_value(
                path=parameter_path, value=parameters[parameter_path])

        # execute the command, we block it, to only get the final response for this sample client
        response = command_object(block=True)

        return_value = {}
        for path in command_object.responses.paths:
            return_value[path] = response.get_value(path=path)

        return return_value

    def call_property(self, feature_id: str,
                      property_id: str) -> Dict[str, Any]:
        _property = self._features[feature_id].properties[property_id]

        if _property.observable:
            for response in _property():
                return_value = {}
                for path in _property.responses.paths:
                    return_value[path] = response.get_value(path=path)

                return return_value
        else:
            # execute the property
            response = _property()

            return_value = {}
            for path in _property.responses.paths:
                return_value[path] = response.get_value(path=path)

            return return_value

    @staticmethod
    def _list_names(content: Dict[str, Any]):
        return list(content.keys())


# if __name__ == "__main__":
     # Add source to path to enable imports
     # import os
     # import sys

     # sys.path.insert(0, os.path.abspath('.'))
     # or use logging.INFO (=20) or logging.ERROR (=30) for less output
     # logging.basicConfig(format='%(levelname)-8s| %(module)s.%(funcName)s: %(message)s', level=logging.INFO)
     # client = DynamicSiLA2Client(name="DynamicClient", server_ip='127.0.0.1', server_port=50051)

     # create the client files
     # client.generate_files()  # Only necessary, if files do not already exist
     # start the client, which will load all data from the server
     # client.run()

#     # Reduce the logging output
#     logging.getLogger().setLevel(logging.ERROR)

#     # List all commands and properties
#     print('Available commands and properties for this server:')
#     for feature in client.list_features():
#         print('Feature: {feature_id}'.format(feature_id=feature))
#         print('  Commands:'.format(feature_id=feature))
#         commands = client.list_commands(feature_id=feature)
#         for command in commands:
#             print('    {command_id}: ({types_in}) -> ({types_out})'.format(
#                 command_id=command.identifier,
#                 # types_in=", ".join(command.input_data_path),
#                 types_in=", ".join(command.input_data_type),
#                 # types_out=", ".join(command.output_data_path)
#                 types_out=", ".join(command.output_data_type)
#             ))
#         print('  Properties:'.format(feature_id=feature))
#         properties = client.list_properties(feature_id=feature)
#         for property_element in properties:
#             print('    {command_id}: () -> ({types_out})'.format(
#                 command_id=property_element.identifier,
#                 # types_out=", ".join(property_element.output_data_path)
#                 types_out=", ".join(property_element.output_data_type)
#             ))

#     # Now first ensure the server is running in simulation mode
#     print('Configuring server to run in simulation mode.')
#     client.switchToSimMode()

#     # And now we offer some user interaction
#     #   Predefine variables we need
#     choice = ''
#     feature = None
#     while choice not in ['q', 'Q', 'Quit', 'quit', 'exit', 'Exit']:
#         # first evaluate the fixed options
#         #   Simulation mode options
#         if choice.lower() == 's':
#             client.switchToSimMode()
#             choice = ''
#         if choice.lower() == 'r':
#             client.switchToRealMode()
#             choice = ''
#         if choice.lower() == 't':
#             client.toggleSimMode()
#             choice = ''
#         #   User interaction options
#         if choice.lower() == 'x':
#             feature = None
#             choice = ''
#         #   Feature
#         if feature is None:
#             # if choice got as far as here, it might be a feature
#             #   input as a feature name
#             if choice in client.list_features():
#                 feature = choice
#                 choice = ''
#             # input as a list index
#             try:
#                 feature = client.list_features()[int(choice)]
#                 choice = ''
#             except ValueError:
#                 # nothing did work, ignore this
#                 print('!!! Invalid input: "{input}" !!!'.format(input=choice))
#                 feature = None
#                 choice = ''
#         # feature is set, must be command/property
#         else:
#             try:
#                 index = int(choice)
#             except ValueError:
#                 # could not read the correct index
#                 print('!!! Could not read input as an index: "{input}" !!!'.format(input=choice))
#                 choice = ''
#             else:
#                 if index + 1 <= client.count_commands(feature_id=feature):
#                     # is a command
#                     command = client.list_commands(feature_id=feature)[index]
#                     # we need to read the commands parameters
#                     parameter_dict = {}
#                     for parameter_required in command.input_data_path:
#                         param = input('Please enter a value for the commands parameter with the path "{path}": '.format(
#                             path=parameter_required))
#                         parameter_dict[parameter_required] = param

#                     results = client.call_command(feature_id=feature, command_id=command.identifier,
#                                                   parameters=parameter_dict)

#                     print('The server returned the following result(s):')
#                     for result in results:
#                         print('  {path}: {value}'.format(path=result, value=results[result]))
#                     print('\n')

#                 elif index + 1 <= client.count_commands(feature_id=feature) \
#                         + client.count_properties(feature_id=feature):
#                     # property
#                     prop = client.list_properties(feature_id=feature)[index - client.count_commands(feature_id=feature)]
#                     # we can execute the property directly, it has no parameters
#                     results = client.call_property(feature_id=feature, property_id=prop.identifier)

#                     print('The server returned the following result(s):')
#                     for result in results:
#                         print('  {path}: {value}'.format(path=result, value=results[result]))
#                     print('\n')

#         # Give the current options
#         print('Please choose an action of the following actions:')
#         print()

#         if feature is None:
#             print('The following features can be accessed by their number or name:')

#             for index, feature_name in enumerate(client.list_features(), 0):
#                 print('  {index:2}: {feature_name}'.format(index=index, feature_name=feature_name))
#         else:
#             print('The feature {feature_name} provides the following access methods which can be accessed by number:')
#             print('  Commands (input parameter types):')
#             index = 0
#             for index, command in enumerate(client.list_commands(feature_id=feature), 0):
#                 print('    {index:3}: {command_name} ({parameters})'.format(
#                     index=index, command_name=command.identifier,
#                     parameters=", ".join(command.input_data_type)
#                 ))
#             print('  Properties:')
#             for index, prop in enumerate(client.list_properties(feature_id=feature), index + 1):
#                 print('    {index:3}: {property_name}'.format(
#                     index=index, property_name=prop.identifier))

#         print()
#         print('Server mode options')
#         print('  [s]: Switch to simulation mode')
#         print('  [r]: Switch to real mode')
#         print('  [t]: Toggle simulation mode')

#         print()
#         print('General options:')
#         print('  [x]: Reset current options and go back to the start')
#         print('  [q]: Quit this client')

#         # request user input
#         choice = input('Your choice: ')

#     print('Exiting')
