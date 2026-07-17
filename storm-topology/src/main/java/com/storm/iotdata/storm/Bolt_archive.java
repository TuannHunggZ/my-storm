package com.storm.iotdata.storm;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Map;

/**
 * Sink bolt that persists realtime load events into PostgreSQL historical_load.
 * It only archives data and does not perform any aggregation or business logic.
 */
public class Bolt_archive extends BaseRichBolt {

	private static final Logger LOGGER = LoggerFactory.getLogger(Bolt_archive.class);

	private static final int BATCH_SIZE = 1000;
	private static final long FLUSH_INTERVAL_MS = 1000L;

	private static final String INSERT_SQL =
		"INSERT INTO historical_load (event_id, timestamp, value, plug_id, household_id, house_id) " +
		"VALUES (?, ?, ?, ?, ?, ?)";

	private final String jdbcUrl;
	private final String username;
	private final String password;

	private transient OutputCollector collector;
	private transient Connection connection;
	private transient PreparedStatement preparedStatement;
	private transient int batchCount;
	private transient long lastFlushTimeMs;

	/**
	 * Creates the bolt with the database connection settings.
	 *
	 * @param jdbcUrl PostgreSQL JDBC URL.
	 * @param username Database username.
	 * @param password Database password.
	 */
	public Bolt_archive(String jdbcUrl, String username, String password) {
		this.jdbcUrl = jdbcUrl;
		this.username = username;
		this.password = password;
	}

	/**
	 * Prepares the PostgreSQL connection, disables auto-commit, and creates the prepared statement.
	 *
	 * @param stormConf Storm configuration map.
	 * @param context Topology context.
	 * @param collector Storm output collector used for ack/fail.
	 */
	@Override
	public void prepare(Map<String, Object> stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.batchCount = 0;
		this.lastFlushTimeMs = System.currentTimeMillis();

		try {
			this.connection = DriverManager.getConnection(jdbcUrl, username, password);
			this.connection.setAutoCommit(false);
			this.preparedStatement = this.connection.prepareStatement(INSERT_SQL);

			LOGGER.info("Connected to PostgreSQL archive database {}", jdbcUrl);
		} catch (SQLException exception) {
			cleanupResources();
			throw new IllegalStateException("Unable to prepare PostgreSQL archive bolt", exception);
		}
	}

	/**
	 * Archives one incoming tuple into the PostgreSQL batch buffer and flushes when needed.
	 *
	 * @param tuple Incoming Storm tuple from the data stream.
	 */
	@Override
	public void execute(Tuple tuple) {
		try {
			flushIfTimedOut();
			addToBatch(tuple);
			flushIfBatchFull();
			collector.ack(tuple);
		} catch (SQLException exception) {
			handleDatabaseFailure(tuple, exception);
		} catch (RuntimeException exception) {
			collector.fail(tuple);
			throw exception;
		}
	}

	/**
	 * Declares that this bolt does not emit any output streams.
	 *
	 * @param declarer Storm declarer.
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// Sink bolt: no downstream streams.
	}

	/**
	 * Flushes any pending batch and closes JDBC resources when the topology shuts down.
	 */
	@Override
	public void cleanup() {
		try {
			flushBatch();
		} catch (SQLException exception) {
			LOGGER.error("Failed to flush PostgreSQL batch during cleanup", exception);
		} finally {
			cleanupResources();
		}
	}

	private void addToBatch(Tuple tuple) throws SQLException {
		long eventId = getLongField(tuple, "id");
		long timestampSeconds = getLongField(tuple, "timestamp");
		double value = getDoubleField(tuple, "value");
		int plugId = getIntField(tuple, "plugId");
		int householdId = getIntField(tuple, "householdId");
		int houseId = getIntField(tuple, "houseId");

		preparedStatement.setLong(1, eventId);
		preparedStatement.setTimestamp(2, new Timestamp(timestampSeconds * 1000L));
		preparedStatement.setDouble(3, value);
		preparedStatement.setInt(4, plugId);
		preparedStatement.setInt(5, householdId);
		preparedStatement.setInt(6, houseId);
		preparedStatement.addBatch();

		batchCount += 1;
	}

	private void flushIfBatchFull() throws SQLException {
		if (batchCount >= BATCH_SIZE) {
			flushBatch();
		}
	}

	private void flushIfTimedOut() throws SQLException {
		if (batchCount == 0) {
			return;
		}

		long elapsedMs = System.currentTimeMillis() - lastFlushTimeMs;
		if (elapsedMs >= FLUSH_INTERVAL_MS) {
			flushBatch();
		}
	}

	private void flushBatch() throws SQLException {
		if (batchCount == 0) {
			return;
		}

		try {
			preparedStatement.executeBatch();
			connection.commit();
			LOGGER.info("Flushed {} records to historical_load", batchCount);
		} catch (SQLException exception) {
			try {
				connection.rollback();
			} catch (SQLException rollbackException) {
				LOGGER.warn("Failed to rollback PostgreSQL transaction", rollbackException);
			}
			throw exception;
		} finally {
			preparedStatement.clearBatch();
			batchCount = 0;
			lastFlushTimeMs = System.currentTimeMillis();
		}
	}

	private void handleDatabaseFailure(Tuple tuple, SQLException exception) {
		LOGGER.error("Failed to insert tuple into PostgreSQL archive", exception);
		collector.fail(tuple);
	}

	private long getLongField(Tuple tuple, String fieldName) {
		return tuple.getLongByField(fieldName);
	}

	private int getIntField(Tuple tuple, String fieldName) {
		return tuple.getIntegerByField(fieldName);
	}

	private double getDoubleField(Tuple tuple, String fieldName) {
		return tuple.getDoubleByField(fieldName);
	}

	private void cleanupResources() {
		if (preparedStatement != null) {
			try {
				preparedStatement.close();
			} catch (SQLException exception) {
				LOGGER.warn("Failed to close prepared statement", exception);
			} finally {
				preparedStatement = null;
			}
		}

		if (connection != null) {
			try {
				connection.close();
			} catch (SQLException exception) {
				LOGGER.warn("Failed to close PostgreSQL connection", exception);
			} finally {
				connection = null;
			}
		}
	}
}
