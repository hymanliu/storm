package com.hyman.storm.redis.bolt;

import java.util.Map;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import com.hyman.storm.redis.util.RedisContext;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;

public class CounterBolt extends BaseBasicBolt {
	
	
//	private JedisCluster jedis = null;
	private Jedis jedis = null;
	
	private static final long serialVersionUID = -5508421065181891596L;
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
//		jedis = RedisClusterContext.getInstance().getJedisCluster(this);
		jedis = RedisContext.getInstance().getJedis(this);
	}

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		long num = jedis.incr("msg");
		System.out.println("msg = "+tuple.getString(0)+" -------------counter = "+(num));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

	@Override
	public void cleanup() {
		String num = jedis.get("msg");
		System.out.println("total message----------:"+num);
//		RedisClusterContext.getInstance().releaseRedisConnector(this);
		RedisContext.getInstance().releaseRedisConnector(this);
	}

}
