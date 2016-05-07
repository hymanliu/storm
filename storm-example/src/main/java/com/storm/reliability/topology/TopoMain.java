package com.storm.reliability.topology;

import com.storm.reliability.bolt.FileWriterBolt;
import com.storm.reliability.bolt.SpliterBolt;
import com.storm.reliability.spout.MessageSpout;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class TopoMain {
	
	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new MessageSpout());
		builder.setBolt("bolt-1", new SpliterBolt()).shuffleGrouping("spout");
		builder.setBolt("bolt-2", new FileWriterBolt()).shuffleGrouping("bolt-1");
		Config conf = new Config();
		conf.setDebug(false);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("reliability", conf, builder.createTopology());
	}
}
