# Use a base image
FROM eclipse-mosquitto:latest

# Set the working directory
WORKDIR /mqtt-broker

# Copy the configuration file
COPY mosquitto.conf /etc/mosquitto/mosquitto.conf

# Expose the MQTT port
EXPOSE 1883

# Start the MQTT broker
CMD ["mosquitto", "-c", "/etc/mosquitto/mosquitto.conf"]