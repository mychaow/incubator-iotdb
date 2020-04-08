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

# JMX工具

Java VisualVM提供了一个可视化的界面，用于查看Java应用程序在Java虚拟机（JVM）上运行的详细信息，并对这些应用程序进行故障排除和分析。

## 配置

JMX 访问权限认证设置默认打开，因此你需要编辑`iotdb-engine.properties`中的 JMX 用户名和密码。相关配置项包括：

* jmx\_user

|名字| jmx\_user |
|:---:|:---|
|描述| JMX配置中的用户名 |
|类型| String |
|默认值| admin |
|改后生效方式|重启服务器生效|

* jmx\_password

|名字| jmx\_password |
|:---:|:---|
|描述| JMX配置中的密码 |
|类型| String |
|默认值| password |
|改后生效方式|重启服务器生效|

## 使用

第一步：启动IoTDB server。

第二步：建立连接。对于本地的监控，不需要手动配置连接。对于远程的监控，可以添加远程的ip地址，并将端口配置为`31999`。

第三步：开始监控。双击你所建立的ip地址，就可以看到执行应用的详细信息。
