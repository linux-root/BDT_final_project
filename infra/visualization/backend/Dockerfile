FROM eclipse-temurin:11-jre
WORKDIR /app
COPY visualization/backend/target/scala-3.7.3/backend-assembly.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]
