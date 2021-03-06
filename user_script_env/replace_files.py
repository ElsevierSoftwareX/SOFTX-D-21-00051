import os
from shutil import copyfile

# Retrieve the path to the sila2lib module
try:
    import sila2lib
except ModuleNotFoundError as error:
    print("sila2lib does not appear to be installed or you are not in the correct virtual environment")
    raise

sila2lib_path = os.path.dirname(sila2lib.__file__)

hugo_repository_path = os.path.dirname(os.path.abspath(__file__))

# Copy the files
# sila_server.py
copyfile(hugo_repository_path + '/SiLA_replacements/sila_server.py', sila2lib_path + '/sila_server.py')
# sila_service_detection.py
copyfile(hugo_repository_path + '/SiLA_replacements/sila_service_detection.py', sila2lib_path + '/sila_service_detection.py')
# SiLAService_pb2_grpc.py
copyfile(hugo_repository_path + '/SiLA_replacements/SiLAService_pb2_grpc.py', sila2lib_path + '/framework/std_features/SiLAService_pb2_grpc.py')
# SiLAService_pb2.py
copyfile(hugo_repository_path + '/SiLA_replacements/SiLAService_pb2.py', sila2lib_path + '/framework/std_features/SiLAService_pb2.py')
# SiLAService.py
copyfile(hugo_repository_path + '/SiLA_replacements/SiLAService.py', sila2lib_path + '/framework/std_features/SiLAService.py')
# data_basic.py
copyfile(hugo_repository_path + '/SiLA_replacements/data_basic.py', sila2lib_path + '/proto_builder/data/data_basic.py')
# _dynamic_command.py
copyfile(hugo_repository_path + '/SiLA_replacements/_dynamic_command.py', sila2lib_path + '/proto_builder/_dynamic_command.py')

# fdl_parser.py
copyfile(hugo_repository_path + '/SiLA_replacements/fdl_parser.py', sila2lib_path + '/fdl_parser/fdl_parser.py')
# command.py
copyfile(hugo_repository_path + '/SiLA_replacements/command.py', sila2lib_path + '/fdl_parser/command.py')
# data_base.py
copyfile(hugo_repository_path + '/SiLA_replacements/data_base.py', sila2lib_path + '/proto_builder/data/data_base.py')
# data_structure.py
copyfile(hugo_repository_path + '/SiLA_replacements/data_structure.py', sila2lib_path + '/proto_builder/data/data_structure.py')
# data_type_definition.py
copyfile(hugo_repository_path + '/SiLA_replacements/data_type_definition.py', sila2lib_path + '/fdl_parser/data_type_definition.py')
# data_type_parameter.py
copyfile(hugo_repository_path + '/SiLA_replacements/data_type_parameter.py', sila2lib_path + '/fdl_parser/data_type_parameter.py')
# type_base.py
copyfile(hugo_repository_path + '/SiLA_replacements/type_base.py', sila2lib_path + '/fdl_parser/type_base.py')
# type_basic.py
copyfile(hugo_repository_path + '/SiLA_replacements/type_basic.py', sila2lib_path + '/fdl_parser/type_basic.py')
