
Kafka Tools - A way to test and manage Kafka Brokers and Clusters
=================================================================

Kafka Tools is a suite of tools to help test that your Kafka Cluster is fit for purpose
Also, this has validation scripts that help us validate we have data coming into Kafka.

Installing the Scripts
=======================

The scripts are command line based, designed to be used within a batch script or DevOPs tool such as Chef or Ansible.
Written in python3, all scripts are listed below, and there is a Pipfile to show what modules are required to run.

You will need to use Python 3.6 or higher and the modules listed in the dependency section.  

Please follow the following steps to install:

    1. Download and install python 3.6 or higher from python.org. Append python3 to the LIB and PATH env.

    2. Download and install git for your platform if you don't already have it installed.
       It can be downloaded from https://git-scm.com/downloads
    
    3. Open a new shell/command prompt. It must be new since only a new shell will include the new python 
       path that was created in step 1. Cd to the folder where you want to install the scripts.
    
    4. Execute the following command to install pipenv, which will manage all of the library dependencies:
    
        sudo -H python3 -m pip install pipenv 
 
    5. Clone this repository. This will create a new folder

    6. Change into the new folder. Type the following to install all the package dependencies 
       (this may take a while as this will download all of the libraries required):

        pipenv install
        
Dependencies
============

See the contents of "pipfile"

Script Names and Purposes
=========================

Scripts and Functions:

```
bin
├── kafka_consumer_test.py
├── kafka_producer_test.py
└── syslog_parser_validator.py
```
* kafka_consumer_test.py is a basic consumer of Kafka messages. This will print summary or details

* kafka_producer_test.py this is a basic message creator that can be used as both unit test and load test

* syslog_parser_validator.py this validates the number of syslog messages, and computes a histogram of the data sizes

To Do List:
===========

* Extend the scripts to include more data validation scripts

* Extend the scripts to include tools to reinitialize the databases

License
=======

Copyright 2023 Wayne Kirk Schmidt
https://www.linkedin.com/in/waynekirkschmidt

Licensed under the Apache 2.0 License (the "License");

You may not use this file except in compliance with the License.
You may obtain a copy of the License at

    license-name   APACHE 2.0
    license-url    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Support
=======

Feel free to e-mail me with issues to: 

*   wayne.kirk.schmidt@changeis.co.jp

*   wayne.kirk.schmidt@gmail.com

I will provide "best effort" fixes and extend the scripts.
