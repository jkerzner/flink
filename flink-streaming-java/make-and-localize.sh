#!/bin/bash

mvn -e clean package -DskipTests && mvn -DartifactId=flink-streaming-java_2.10 -DgroupId=org.apache.flink -Dversion=1.1.3 -Dpackaging=jar -Dfile=target/flink-streaming-java_2.11-1.1.3.jar -DgeneratePom=false install:install-file
