package com.hyman.storm.memcached.topology;

import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;

import com.google.common.collect.ImmutableList;
import com.hyman.storm.memcached.bolt.CheckOrderBolt;
import com.hyman.storm.memcached.bolt.SaveMysqlBolt;
import com.hyman.storm.memcached.bolt.TranslateBolt;

public class OrderTopology {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try{
			String kafkaZookeeper = "hyman0:2181,hyman1:2181,hyman2:2181:/kafka";
			BrokerHosts brokerHosts = new ZkHosts(kafkaZookeeper);
			SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, "order", "/order", "id");
	        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
	        kafkaConfig.zkServers =  ImmutableList.of("hyman0","hyman1","hyman2");
	        kafkaConfig.zkPort = 2181;
			
	        //kafkaConfig.forceFromStart = true;
			
	        TopologyBuilder builder = new TopologyBuilder();
	        builder.setSpout("spout", new KafkaSpout(kafkaConfig), 2);
	        builder.setBolt("check", new CheckOrderBolt(),1).shuffleGrouping("spout");
	        builder.setBolt("translate", new TranslateBolt(),3).shuffleGrouping("check");
	        builder.setBolt("save", new SaveMysqlBolt(),3).shuffleGrouping("translate");
	        
	        
	        Config config = new Config();
	        config.setDebug(true);
	        
	        if(args!=null && args.length > 0) {
	            config.setNumWorkers(2);
	            
	            StormSubmitter.submitTopology(args[0], config, builder.createTopology());
	        } else {        
	            config.setMaxTaskParallelism(3);
	
	            LocalCluster cluster = new LocalCluster();
	            cluster.submitTopology("special-topology", config, builder.createTopology());
	        
	            Thread.sleep(500000);
	
	            cluster.shutdown();
	        }
		}catch (Exception e) {
			e.printStackTrace();
		}

	}

}
