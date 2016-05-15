package com.hyman.storm.news.reliabilty;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

public class MainTopology {
	
	static final String MESSAGE_SPOUT = "message-spout";
	static final String SPLIT_BOLT = "split-bolt";
	static final String EXPORT_BOLT = "export-bolt";

	public static void main(String[] args) throws Exception {
		
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout(MESSAGE_SPOUT, new MessageSpout(), 1).setNumTasks(1);
		builder.setBolt(SPLIT_BOLT, new SplitBolt(),1).setNumTasks(1).shuffleGrouping(MESSAGE_SPOUT);
		builder.setBolt(EXPORT_BOLT, new ExportBolt(),1).setNumTasks(1).fieldsGrouping(SPLIT_BOLT, new Fields("word"));
		
		Config conf = new Config();
		
		//关闭storm 的 ack 机制 , Config.TOPOLOGY_ACKER_EXECUTORS=0
		//conf.setNumAckers(0);

		conf.setNumAckers(1);
		conf.setDebug(false);
		if(args!=null && args.length > 0) {
			conf.setNumWorkers(2);
			StormSubmitter.submitTopology(args[0], conf,builder.createTopology());
		} else {
	        LocalCluster cluster = new LocalCluster();
	        cluster.submitTopology("reliabily-topology", conf, builder.createTopology());
	        Utils.sleep(60000);
	        cluster.killTopology("reliabily-topology");
	        cluster.shutdown();
	        System.exit(0);
		}

	}

}
