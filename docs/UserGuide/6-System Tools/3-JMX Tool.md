<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at
    
        http://www.apache.org/licenses/LICENSE-2.0
    
    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

# JMX Tool

Java VisualVM is a tool that provides a visual interface for viewing detailed information about Java applications while they are running on a Java Virtual Machine (JVM), and for troubleshooting and profiling these applications. 

## Config

JMX authenticate configuration is `true` by default, so you need to edit JMX user name and password in `iotdb-engine.properties`. Related configurations are:

* jmx\_user

|Name| jmx\_user |
|:---:|:---|
|Description| User name of JMX |
|Type| String |
|Default| admin |
|Effective|After restart system|

* jmx\_password

|Name| jmx\_password |
|:---:|:---|
|Description| User password of JMX |
|Type| String |
|Default| password |
|Effective|After restart system|

## Usage

Step1: Start sever.

Step2: Build connection. For local monitor, connection doesn't need to be built manually. For remote monitor, you can add your own ip address, and config the port. For IoTDB, port is `31999`.

Step3: Start monitoring. By double clicking your connection ip address, you can see the detailed execution information about the application.
