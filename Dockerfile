FROM gradle:8.4.0-jdk21 AS builder

# Set the working directory inside the container
WORKDIR /app

# Copy Gradle files and source code
COPY build.gradle settings.gradle ./
COPY gradle ./gradle
COPY gradlew ./
COPY gradlew.bat ./
COPY src ./src

# Build the project
RUN ./gradlew build --no-daemon

# ---------------------------------------------

# Use a smaller runtime image for final container
FROM eclipse-temurin:21-jdk AS runtime

# Set working directory
WORKDIR /app

# Copy the jar from the builder stage
COPY --from=builder /app/build/libs/JavaBIGO-0.0.1-SNAPSHOT.jar app.jar

# Set environment variables (can be overridden when running)
ENV CURRENT_NODE_IP=127.0.0.1
ENV ALL_NODE_IPS=127.0.0.1
ENV PORT=4001

# Expose the default port (you can change this if needed)

# Start the application with environment variables
ENTRYPOINT ["sh", "-c", "java -DSERVER_PORT=$PORT -DCURRENT_NODE_IP=$CURRENT_NODE_IP -DALL_NODE_IPS=$ALL_NODE_IPS -jar app.jar"]
