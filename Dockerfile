# Gemeinsames Multi-Stage-Dockerfile für beide Services; Modul per Build-Arg MODULE.
# Ohne BuildKit-Features, damit es auch mit Docker 19.03 (klassischer Builder) baut.
FROM maven:3.9-eclipse-temurin-17-focal AS build
ARG MODULE
WORKDIR /build
# Erst nur die POMs kopieren und Abhängigkeiten laden: eigener Layer, bleibt bei Code-Änderungen gecacht.
COPY pom.xml .
COPY kafka-producer/pom.xml kafka-producer/
COPY kafka-consumer/pom.xml kafka-consumer/
RUN mvn -B -q -pl ${MODULE} -am dependency:go-offline
COPY kafka-producer/src kafka-producer/src
COPY kafka-consumer/src kafka-consumer/src
RUN mvn -B -q -pl ${MODULE} -am package -DskipTests

FROM eclipse-temurin:17-jre-focal
ARG MODULE
WORKDIR /app
COPY --from=build /build/${MODULE}/target/app.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]
