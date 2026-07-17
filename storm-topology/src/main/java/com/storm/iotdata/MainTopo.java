package com.storm.iotdata;

import org.apache.storm.Config;

public class MainTopo {

	/**
	 * Creates the Storm configuration used by this topology.
	 *
	 * @return Storm config with tick tuples enabled every second.
	 */
	public static Config createConfig() {
		Config config = new Config();
		config.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 1);
		return config;
	}
}