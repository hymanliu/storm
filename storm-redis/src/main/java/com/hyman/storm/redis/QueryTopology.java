package com.hyman.storm.redis;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.StormSubmitter;
import org.apache.storm.drpc.LinearDRPCTopologyBuilder;

import com.hyman.storm.redis.bolt.QueryBolt;



@SuppressWarnings("deprecation")
public class QueryTopology {

	public static void main(String[] args) throws Exception {
		LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("query-word");
	    
	    builder.addBolt(new QueryBolt(), 1);
	 
	    Config conf = new Config();
	 
	    if (args == null || args.length == 0) {
	      LocalDRPC drpc = new LocalDRPC();
	      LocalCluster cluster = new LocalCluster();
	 
	      cluster.submitTopology("drpc-count-query", conf, builder.createLocalTopology(drpc));
	 
	      while(true){
		      for (String word : new String[]{ "hello", "goodbye" }) {
		        System.out.println("Result for \"" + word + "\": " + drpc.execute("query-word", word));
		      }
	      }
	 
	      //cluster.shutdown();
	      //drpc.shutdown();
	    }
	    else {
	      conf.setNumWorkers(3);
	      StormSubmitter.submitTopology(args[0], conf, builder.createRemoteTopology());
	    }
	  }
}
