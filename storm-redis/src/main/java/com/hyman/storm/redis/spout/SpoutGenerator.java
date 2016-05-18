package com.hyman.storm.redis.spout;

import java.util.Arrays;

import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;

import com.hyman.storm.redis.util.ApplicationContext;

public class SpoutGenerator {

	public static KafkaSpout create(){
		String kafkaZookeeper = ApplicationContext.getConfig("kafka.zookeeper.connect");
		String topic = ApplicationContext.getConfig("kafka.topic");
		BrokerHosts brokerHosts = new ZkHosts(kafkaZookeeper);
		
		String zkRoot = ApplicationContext.getConfig("zookeeper.rootpath");
		
		SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, topic, zkRoot, "id");
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        
        String zkServers = ApplicationContext.getConfig("zookeeper.servers");
        String[] serversArr = zkServers.split(",");
        kafkaConfig.zkServers = Arrays.asList(serversArr);
        kafkaConfig.zkPort = Integer.parseInt(ApplicationContext.getConfig("zookeeper.port"));
        //kafkaConfig.forceFromStart = true;
        
        return new KafkaSpout(kafkaConfig);
	}
}
