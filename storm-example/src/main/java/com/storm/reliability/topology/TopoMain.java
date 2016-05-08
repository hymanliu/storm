package com.storm.reliability.topology;

import com.storm.reliability.bolt.FileWriterBolt;
import com.storm.reliability.bolt.SpliterBolt;
import com.storm.reliability.spout.MessageSpout;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class TopoMain {
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Config conf = new Config();
		conf.setNumWorkers(2);//设置worker的数量
		
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new MessageSpout(),2)//设置并行度为2 也就是有2个excutor(注意这里并不是2个Spout)
		.setNumTasks(4);//设置Task的数量，4个Spout Task ，分配到两个Excutor上，每个Excutor分配两个Task
		
		
		builder.setBolt("bolt-1", new SpliterBolt(),2).shuffleGrouping("spout");
		builder.setBolt("bolt-2", new FileWriterBolt(),2).setNumTasks(4).shuffleGrouping("bolt-1");//.fieldsGrouping("bolt-1", new Fields("word"));
		
		conf.setDebug(false);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("reliability", conf, builder.createTopology());
	}
}
