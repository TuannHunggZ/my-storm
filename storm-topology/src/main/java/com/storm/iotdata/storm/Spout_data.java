package com.storm.iotdata.storm;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Storm spout that subscribes to MQTT, parses incoming JSON messages, filters
 * only load events (property == 1), and emits tuples on the "data" stream.
 */
public class Spout_data extends BaseRichSpout {

	private static final Logger LOGGER = LoggerFactory.getLogger(Spout_data.class);

	private static final String DEFAULT_BROKER_URI = "tcp://localhost:1883";
	private static final String DEFAULT_TOPIC = "iot-data";
	private static final int DEFAULT_QOS = 0;
	private static final int DEFAULT_CONNECTION_TIMEOUT_SECONDS = 10;
	private static final int PROPERTY_LOAD = 1;
	private static final int MAX_EMIT_PER_NEXT_TUPLE = 100;
	private static final int QUEUE_CAPACITY = 10_000;
	private static final String STREAM_ID_DATA = "data";
	private static final String FIELD_ID = "id";
	private static final String FIELD_TIMESTAMP = "timestamp";
	private static final String FIELD_VALUE = "value";
	private static final String FIELD_PLUG_ID = "plugId";
	private static final String FIELD_HOUSEHOLD_ID = "householdId";
	private static final String FIELD_HOUSE_ID = "houseId";

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private final String brokerUri;
	private final String topic;
	private final int qos;

	private transient SpoutOutputCollector collector;
	private transient MqttClient mqttClient;
	private final BlockingQueue<LoadEvent> eventQueue = new LinkedBlockingQueue<>(QUEUE_CAPACITY);

	/**
	 * Creates a spout with the default broker URI and topic.
	 */
	public Spout_data() {
		this(DEFAULT_BROKER_URI, DEFAULT_TOPIC, DEFAULT_QOS);
	}

	/**
	 * Creates a spout with custom MQTT connection settings.
	 *
	 * @param brokerUri MQTT broker URI.
	 * @param topic MQTT topic to subscribe to.
	 * @param qos MQTT QoS level.
	 */
	public Spout_data(String brokerUri, String topic, int qos) {
		this.brokerUri = brokerUri;
		this.topic = topic;
		this.qos = qos;
	}

	/**
	 * Initializes the MQTT client, registers the callback, and subscribes to the configured topic.
	 *
	 * @param conf Storm configuration map.
	 * @param context Topology context.
	 * @param collector Storm spout collector used for emitting tuples.
	 */
	@Override
	public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		initializeMqttClient();
	}

	/**
	 * Drains a bounded number of parsed MQTT events from the queue and emits them to the data stream.
	 */
	@Override
	public void nextTuple() {
		int emitted = 0;

		while (emitted < MAX_EMIT_PER_NEXT_TUPLE) {
			LoadEvent event = eventQueue.poll();
			if (event == null) {
				break;
			}

			collector.emit(
				STREAM_ID_DATA,
				new Values(
					event.id,
					event.timestamp,
					event.value,
					event.plugId,
					event.householdId,
					event.houseId
				),
				event.id
			);
			emitted += 1;
		}

		if (emitted == 0) {
			Utils.sleep(1);
		}
	}

	/**
	 * Declares the output fields for the "data" stream.
	 *
	 * @param declarer Storm declarer.
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(
			STREAM_ID_DATA,
			new Fields(
				FIELD_ID,
				FIELD_TIMESTAMP,
				FIELD_VALUE,
				FIELD_PLUG_ID,
				FIELD_HOUSEHOLD_ID,
				FIELD_HOUSE_ID
			)
		);
	}

	/**
	 * Disconnects and closes the MQTT client.
	 */
	@Override
	public void close() {
		if (mqttClient == null) {
			return;
		}

		try {
			mqttClient.disconnect();
		} catch (MqttException exception) {
			LOGGER.warn("Failed to disconnect MQTT client cleanly", exception);
		} finally {
			try {
				mqttClient.close();
			} catch (MqttException exception) {
				LOGGER.warn("Failed to close MQTT client", exception);
			}
			mqttClient = null;
		}
	}

	/**
	 * Reliability is not required for this spout.
	 *
	 * @param msgId Storm message identifier.
	 */
	@Override
	public void ack(Object msgId) {
		// TODO: Reliability is not required for this spout.
	}

	/**
	 * Reliability is not required for this spout.
	 *
	 * @param msgId Storm message identifier.
	 */
	@Override
	public void fail(Object msgId) {
		// TODO: Reliability is not required for this spout.
	}

	private void initializeMqttClient() {
		try {
			String clientId = "storm-spout-" + UUID.randomUUID();
			mqttClient = new MqttClient(brokerUri, clientId);

			MqttConnectOptions connectOptions = new MqttConnectOptions();
			connectOptions.setAutomaticReconnect(true);
			connectOptions.setCleanSession(true);
			connectOptions.setConnectionTimeout(DEFAULT_CONNECTION_TIMEOUT_SECONDS);

			mqttClient.setCallback(new MqttCallbackExtended() {
				@Override
				public void connectComplete(boolean reconnect, String serverURI) {
					if (reconnect) {
						LOGGER.info("MQTT reconnected to {}", serverURI);
					} else {
						LOGGER.info("MQTT connected to {}", serverURI);
					}

					subscribeToTopic();
				}

				@Override
				public void connectionLost(Throwable cause) {
					LOGGER.warn("MQTT connection lost", cause);
				}

				@Override
				public void messageArrived(String incomingTopic, MqttMessage message) {
					handleIncomingMessage(incomingTopic, message);
				}

				@Override
				public void deliveryComplete(IMqttDeliveryToken token) {
					// Not used by this spout.
				}
			});

			mqttClient.connect(connectOptions);
		} catch (MqttException exception) {
			throw new IllegalStateException("Unable to initialize MQTT client", exception);
		}
	}

	private void subscribeToTopic() {
		if (mqttClient == null || !mqttClient.isConnected()) {
			return;
		}

		try {
			mqttClient.subscribe(topic, qos);
			LOGGER.info("Subscribed to MQTT topic {} with qos {}", topic, qos);
		} catch (MqttException exception) {
			LOGGER.error("Failed to subscribe to topic {}", topic, exception);
		}
	}

	private void handleIncomingMessage(String incomingTopic, MqttMessage message) {
		try {
			String payload = new String(message.getPayload(), StandardCharsets.UTF_8);
			JsonNode root = OBJECT_MAPPER.readTree(payload);
			LoadEvent event = parseLoadEvent(root);

			if (event == null) {
				return;
			}

			boolean enqueued = eventQueue.offer(event);
			if (!enqueued) {
				LOGGER.warn("Event queue is full, dropping message from topic {}", incomingTopic);
			}
		} catch (Exception exception) {
			LOGGER.error("Failed to parse MQTT JSON payload from topic {}", incomingTopic, exception);
		}
	}

	private LoadEvent parseLoadEvent(JsonNode root) {
		if (root == null) {
			return null;
		}

		int property = getRequiredInt(root, "property");
		if (property != PROPERTY_LOAD) {
			return null;
		}

		return new LoadEvent(
			getRequiredLong(root, "id"),
			getRequiredLong(root, "timestamp"),
			getRequiredDouble(root, "value"),
			getRequiredInt(root, "plug_id"),
			getRequiredInt(root, "household_id"),
			getRequiredInt(root, "house_id")
		);
	}

	private int getRequiredInt(JsonNode root, String fieldName) {
		JsonNode node = root.get(fieldName);

		if (node == null) {
			throw new IllegalArgumentException("Missing field: " + fieldName);
		}

		try {
			return Integer.parseInt(node.asText());
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException("Invalid integer field: " + fieldName, e);
		}
	}

	private long getRequiredLong(JsonNode root, String fieldName) {
		JsonNode node = root.get(fieldName);
		if (node == null || !node.canConvertToLong()) {
			throw new IllegalArgumentException("Missing or invalid field: " + fieldName);
		}
		return node.asLong();
	}

	private double getRequiredDouble(JsonNode root, String fieldName) {
		JsonNode node = root.get(fieldName);
		if (node == null) {
        	throw new IllegalArgumentException("Missing field: " + fieldName);
		}

		try {
			return Double.parseDouble(node.asText());
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException("Invalid double field: " + fieldName, e);
		}
	}

	/**
	 * Parsed MQTT event that is ready to be emitted into Storm.
	 */
	private static final class LoadEvent {

		private final long id;
		private final long timestamp;
		private final double value;
		private final int plugId;
		private final int householdId;
		private final int houseId;

		private LoadEvent(long id, long timestamp, double value, int plugId, int householdId, int houseId) {
			this.id = id;
			this.timestamp = timestamp;
			this.value = value;
			this.plugId = plugId;
			this.householdId = householdId;
			this.houseId = houseId;
		}
	}
}
