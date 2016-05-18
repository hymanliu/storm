package com.hyman.storm.redis;

import com.hyman.storm.redis.bolt.CounterBolt;
import com.hyman.storm.redis.spout.SpoutGenerator;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

public class MainTopology {

	public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();
        KafkaSpout kafkaSpout = SpoutGenerator.create();
        builder.setSpout("spout", kafkaSpout, 1);
        
        builder.setBolt("counter", new CounterBolt(),1).shuffleGrouping("spout");
        
        Config config = new Config();
        config.setDebug(false);
        
        if(args!=null && args.length > 0) {
            config.setNumWorkers(2);
            StormSubmitter.submitTopology(args[0], config, builder.createTopology());
        } else {        
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("count-test", config, builder.createTopology());
//            Utils.sleep(20000);
//            cluster.killTopology("count-test");
//            cluster.shutdown();
//            System.exit(0);
            
        }

	}

}
