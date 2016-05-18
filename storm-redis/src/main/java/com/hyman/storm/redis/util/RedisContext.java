package com.hyman.storm.redis.util;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

public class RedisContext {
	
	private static RedisContext redisContext;
	private Map<Object,JedisCluster> context;
	private Set<HostAndPort> jedisClusterNodes;

	private Set<HostAndPort> getRedisConfig(){
		Set<HostAndPort> jedisClusterNodes = new HashSet<HostAndPort>();
		String redisNodes = ApplicationContext.getConfig("redis.connect");
		String[] arr = redisNodes.split(",");
		for(String hostport: arr){
			String[] hpArr = hostport.split(":");
			if(hpArr.length==2){
				jedisClusterNodes.add(new HostAndPort(hpArr[0], Integer.parseInt(hpArr[1])));
			}
		}
    	return jedisClusterNodes;
	}
	
	private RedisContext(){
		context = new HashMap<Object,JedisCluster>();
		jedisClusterNodes = getRedisConfig();
	}
	
	
	public JedisCluster getJedisCluster(Object key){
		JedisCluster val = context.get(key);
		if(val==null){
			JedisCluster jc = new JedisCluster(jedisClusterNodes);
			context.put(key, jc);
		}
		return context.get(key);
	}
	
	public void releaseRedisConnector(Object key){
		JedisCluster jc = context.remove(key);
		if(jc!=null){
			try {
				jc.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
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
