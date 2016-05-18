package com.hyman.storm.redis.util;

import java.util.HashMap;
import java.util.Map;

import redis.clients.jedis.Jedis;

public class RedisContext {

	private static RedisContext redisContext;
	private Map<Object,Jedis> connectMap;
	private String redishost;
	private int redisport=6379;

	
	private RedisContext(){
		connectMap = new HashMap<Object,Jedis>();
		String redisNode = ApplicationContext.getConfig("redis.connect");
		String[] hostAndPort = redisNode.split(":");
		redishost = hostAndPort[0];
		if(hostAndPort.length>1){
			redisport = Integer.parseInt(hostAndPort[1]);
		}
	}
	
	
	public Jedis getJedis(Object key){
		Jedis val = connectMap.get(key);
		if(val==null){
			Jedis jc = new Jedis(redishost,redisport);
			connectMap.put(key, jc);
		}
		return connectMap.get(key);
	}
	
	public void releaseRedisConnector(Object key){
		Jedis jc = connectMap.remove(key);
		if(jc!=null){
			jc.close();
		}
	}
	
	public static RedisContext getInstance(){
		synchronized(RedisContext.class){
			if(redisContext ==null){
				redisContext = new RedisContext();
			}
		}
		return redisContext;
	}
	
}
