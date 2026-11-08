# Index pod image (ECS / Kubernetes). Build the jar first: mvn -pl galaxy-index-server -am package
FROM eclipse-temurin:21-jre
WORKDIR /app
COPY target/galaxy-index-server.jar /app/galaxy-index-server.jar
ENV GALAXY_CONFIG=/config/galaxy.yaml
EXPOSE 7070
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-jar", "/app/galaxy-index-server.jar"]
