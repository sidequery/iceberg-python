# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM apache/hive:4.0.0

# Dependency versions - changing these invalidates the JAR download layer
ARG HADOOP_VERSION=3.3.6
ARG AWS_SDK_BUNDLE=1.12.753
ARG MAVEN_MIRROR=https://repo1.maven.org/maven2

USER root

# Download JARs (slow layer - only changes when versions change). ADD fetches them without curl, because the
# image's Debian 11 security repository moved to archive.debian.org and apt-get can no longer install curl.
ADD --chmod=644 ${MAVEN_MIRROR}/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar /opt/hive/lib/
ADD --chmod=644 ${MAVEN_MIRROR}/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_BUNDLE}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE}.jar /opt/hive/lib/

# Copy configuration last (changes more frequently than JARs)
COPY core-site.xml /opt/hadoop/etc/hadoop/core-site.xml

USER hive
