Software Architecture
=====================

General architecture
---------------------
The SiLA 2 Manager is based solely on freely available, open-source code. Used packages were selected with long-term
support and respective low maintenance considerations.

The current implementation is mainly focused on the SiLA_python repository but is interoperable with most other implementations
as well (like C++). The SiLA-python repository, and all other repositories for that matter, currently don't implement the
full standard yet. The SiLA 2 implementations are still being actively developed. We work closely together with the
SiLA Working group and some of our members are actively contributing to the programming language specific repositories.
As the SiLA implementation evolve, the SiLA 2 Manager will be updated to incorporate the new changes.

The software system level shows the interaction between the SiLA 2 Manager and other connected software systems.

.. image:: _static/figures/software_system.png
   :width: 800
   :alt: General architecture on a high level of abstraction. The software system level shows the interaction between the SiLA 2 Manager and other connected software systems.

The container view shows the internal structure of the SiLA 2 Manager.

.. image:: _static/figures/sila2_manager.png
   :width: 800
   :alt: Architecture of the SiLA 2 Manager on a more detailed level. The container diagram.

Discovery of SiLA devices
-------------------------------
All SiLA servers implement Multicast DNS (mDNS) and DNS-based Service Discovery (DNS-SD). The SiLA2 specifications for
service discovery are defined in the `SiLA Part (B) - Mapping Specification <https://docs.google.com/document/d/1-shgqdYW4sgYIb5vWZ8xTwCUO_bqE13oBEX8rYY_SJA/edit#heading=h.w2jcp32bd1a5>`_.
The SiLA 2 Manager uses the `python-zeroconf <https://github.com/jstasiak/python-zeroconf>`_ implementation to
discover registered services.

.. note::
        The service description multicasted by mDNS is not the same for all SiLA implementations. At the moment, only the
        services implemented with SiLA 2 python will show the correct  name of the service. However, a change to the SiLA
        standard has been made to obtain the description details from all implementations. This change will be included
        in future releases (09.03.2021). For implementations other than SiLA 2 python, the discovery will display the
        service with the default "unnamed" service name. The displayed name for SiLA python servers is equal to the
        SiLA Server Name.

The backend code used for the discovery feature is located in the folder *source.device_manager.sila_auto_discovery*.

.. automodule:: source.device_manager.sila_auto_discovery.sila_auto_discovery
    :members:
    :undoc-members:
    :show-inheritance:


**Discovery Example**

The discovery functionality can easily be explored by running the *find()* function from the root directory of this project:

.. code-block:: python

    import os
    import sys
    sys.path.insert(0, os.path.abspath('.'))
    from source.device_manager.sila_auto_discovery.sila_auto_discovery import find


    servers = find()
    print(servers)

Dynamic client
---------------
The dynamic client is capable of connecting to a server without any prior knowledge of the servers functionality. In SiLA2
this is made possible by standard features. The SiLA Service feature contains the necessary functions
(Get_ImplementedFeatures(), and Get_FeatureDefinition()) to query the information necessary to construct the client once a
connection has been established.

The SiLA2 Device Manager uses the SiLA_python dynamic client. The created client files are stored as temporary data on the
host machine the device machine is running on. The client files are deleted if the device is deleted within the application.
When a device is added to application, a UUID is assigned for internal reference. This UUID is displayed in the expandable device detail information
on the frontend main page and is used. This UUID is also used as storage name for the device client files.

**Dynamic client Example**

.. automodule:: source.device_manager.device_layer.dynamic_client
    :members: DynamicSiLA2Client
    :undoc-members:
    :show-inheritance:

The dynamic client is located at *source.device_manager.device_layer*. The dynamic client can be executed freely without
the application. If invoked directly, the respective code snippet at the bottom of the file can be un-commented. The basic
connection and code generation functionality can be achieved with the code snippet below. Further examples can be found
in the file itself.

.. code-block:: python

    if __name__ == "__main__":
         # Add source to path to enable imports
         import os
         import sys
         sys.path.insert(0, os.path.abspath('.'))

         # or use logging.INFO (=20) or logging.ERROR (=30) for less output
         # logging.basicConfig(format='%(levelname)-8s| %(module)s.%(funcName)s: %(message)s', level=logging.INFO)
         client = DynamicSiLA2Client(name="DynamicClient", server_ip='127.0.0.1', server_port=50051)

         # create the client files
         client.generate_files()
         # start the client, which will load all data from the server
         client.run()


Backend API
------------

The python backend uses the `FastAPI web framework <https://fastapi.tiangolo.com/>`_. The source code is open-source and
available in the `fastapi repository <https://github.com/tiangolo/fastapi>`_.

.. automodule:: backend
    :members:
