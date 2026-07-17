package com.storm.iotdata;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

import com.storm.iotdata.storm.*;

public class MainTopo {

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout-data", new Spout_data(), 1);
		builder.setBolt("bolt-archive", new Bolt_archive("jdbc:postgresql://localhost:5432/iotdata", "postgres", "postgres"), 1)
			.shuffleGrouping("spout-data", "data");

        Config config = new Config();
        config.setDebug(true);

        LocalCluster cluster = new LocalCluster();

        try {
            cluster.submitTopology(
                "iot-smarthome",
                config,
                builder.createTopology()
            );

            Thread.sleep(10000);
        } finally {
            cluster.close();
        }
    }
}