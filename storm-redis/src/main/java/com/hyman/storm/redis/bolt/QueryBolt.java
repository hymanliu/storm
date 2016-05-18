package com.hyman.storm.redis.bolt;

import java.util.Map;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import com.hyman.storm.redis.util.RedisContext;

import redis.clients.jedis.Jedis;

public class QueryBolt extends BaseBasicBolt {

	private static final long serialVersionUID = -4593547493598669509L;
	
	private Jedis jedis = null;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		jedis = RedisContext.getInstance().getJedis(this);
	}

	@Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
      String input = tuple.getString(1);
      
      String num = jedis.get(input);
      collector.emit(new Values(tuple.getValue(0), num==null ? 0 :num));
    }
 
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("id", "num"));
    }

    @Override
	public void cleanup() {
    	RedisContext.getInstance().releaseRedisConnector(this);
	}
    
}
