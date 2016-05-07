package com.storm.example;

import com.storm.utils.Utils;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class MainTopology {
	
	private static final String SENTENCE_SPOUT_ID = "sentence-spout";
    private static final String SPLIT_BOLT_ID = "split-bolt";
    private static final String COUNT_BOLT_ID = "count-bolt";
    private static final String REPORT_BOLT_ID = "report-bolt";
    private static final String TOPOLOGY_NAME = "word-count-topology";

	public static void main(String[] args) throws Exception {

		SentenceSpout spout = new SentenceSpout();
		SplitSentenceBolt splitBolt = new SplitSentenceBolt();
		WordCountBolt countBolt = new WordCountBolt();
		ReportBolt reportBolt = new ReportBolt();
		
		TopologyBuilder builder = new TopologyBuilder();
		
		builder.setSpout(SENTENCE_SPOUT_ID, spout);
		builder.setBolt(SPLIT_BOLT_ID, splitBolt).shuffleGrouping(SENTENCE_SPOUT_ID);
		builder.setBolt(COUNT_BOLT_ID, countBolt).fieldsGrouping(SPLIT_BOLT_ID, new Fields("word"));
		builder.setBolt(REPORT_BOLT_ID, reportBolt).allGrouping(COUNT_BOLT_ID);
		
		Config conf = new Config();
	
		//关闭storm 的 ack 机制 , Config.TOPOLOGY_ACKER_EXECUTORS=0
		//conf.setNumAckers(0);

		conf.setDebug(true);
		if(args!=null && args.length > 0) {
			conf.setNumWorkers(3);
			StormSubmitter.submitTopology(args[0], conf, 
			builder.createTopology());
		} else {
	        LocalCluster cluster = new LocalCluster();
	        cluster.submitTopology(TOPOLOGY_NAME, conf, builder.createTopology());
	        Utils.waitForSeconds(60);
	        cluster.killTopology(TOPOLOGY_NAME);
	        cluster.shutdown();
		}
	}

}
