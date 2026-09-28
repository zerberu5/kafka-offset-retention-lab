# syntax=docker/dockerfile:1
# Gemeinsames Multi-Stage-Dockerfile für beide Services; Modul per Build-Arg MODULE.
FROM maven:3.9-eclipse-temurin-17 AS build
ARG MODULE
WORKDIR /build
COPY pom.xml .
COPY kafka-producer/pom.xml kafka-producer/
COPY kafka-consumer/pom.xml kafka-consumer/
COPY kafka-producer/src kafka-producer/src
COPY kafka-consumer/src kafka-consumer/src
RUN --mount=type=cache,target=/root/.m2 mvn -B -q -pl ${MODULE} -am package -DskipTests

FROM eclipse-temurin:17-jre
ARG MODULE
WORKDIR /app
COPY --from=build /build/${MODULE}/target/app.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]
