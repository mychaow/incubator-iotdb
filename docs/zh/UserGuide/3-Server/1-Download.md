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

# 下载

IoTDB为您提供了两种安装方式，您可以参考下面的建议，任选其中一种：

第一种，从官网下载安装包。这是我们推荐使用的安装方式，通过该方式，您将得到一个可以立即使用的、打包好的二进制可执行文件。

第二种，使用源码编译。若您需要自行修改代码，可以使用该安装方式。

## 安装环境要求

安装前请保证您的电脑上配有JDK>=1.8的运行环境，并配置好JAVA_HOME环境变量。

如果您需要从源码进行编译，还需要安装：

1. Maven>=3.1的运行环境，具体安装方法可以参考以下链接：[https://maven.apache.org/install.html](https://maven.apache.org/install.html)。

> 注： 也可以选择不安装，使用我们提供的'mvnw.sh' 或 'mvnw.cmd' 工具。使用时请用'mvnw.sh' 或 'mvnw.cmd'命令代替下文的'mvn'命令。

## 从官网下载二进制可执行文件

您可以从[http://iotdb.apache.org/Download/](http://iotdb.apache.org/Download/)上下载已经编译好的可执行程序iotdb-xxx.tar.gz或者iotdb-xxx.zip，该压缩包包含了IoTDB系统运行所需的所有必要组件。

```
NOTE:
iotdb-<version>.tar.gz # For Linux or MacOS
iotdb-<version>.zip # For Windows
```

下载后，您可使用以下操作对IoTDB的压缩包进行解压: 

如果您使用的操作系统是Windows，则使用解压缩工具解压或使用如下解压命令：

```
Shell > uzip iotdb-<version>.zip
```

如果您使用的操作系统是Linux或MacOS，则使用如下解压命令：

```
Shell > tar -zxf iotdb-<version>.tar.gz # For Linux or MacOS
```

解压后文件夹内容见图：

```
server/     <-- root path
|
+- sbin/       <-- script files
|
+- conf/      <-- configuration files
|
+- lib/       <-- project dependencies
|
+- LICENSE    <-- LICENSE
```

## 使用源码编译

您可以获取已发布的源码https://iotdb.apache.org/Download/，或者从git仓库获取https://github.com/apache/incubator-iotdb/tree/master

源码克隆后，进入到源码文件夹目录下，使用以下命令进行编译：

```
> mvn clean package -pl server -am -Dmaven.test.skip=true
```

成功后，可以在终端看到如下信息:

```
[INFO] ------------------------------------------------------------------------
[INFO] Reactor Summary:
[INFO]
[INFO] Apache IoTDB (incubating) Project Parent POM ....... SUCCESS [  6.405 s]
[INFO] TsFile ............................................. SUCCESS [ 10.435 s]
[INFO] Service-rpc ........................................ SUCCESS [  4.170 s]
[INFO] IoTDB Jdbc ......................................... SUCCESS [  3.252 s]
[INFO] IoTDB Server ....................................... SUCCESS [  8.072 s]
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
```

否则，你需要检查错误语句，并修复问题。

编译后，IoTDB 服务器会在 "server/target/iotdb-server-{project.version}" 文件夹下，包含以下内容：

```
$IOTDB_HOME/
|
+- sbin/       <-- script files
|
+- conf/      <-- configuration files
|
+- lib/       <-- project dependencies
```

## 通过Docker安装 (Dockerfile)

你可以通过[这份指南](../3-Server/5-Docker%20Image.html)编译并运行一个IoTDB docker image。
