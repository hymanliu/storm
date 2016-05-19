package com.hyman.storm.redis.util;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

public class RedisClusterContext {
	
	private static RedisClusterContext redisClusterContext;
	private Map<Object,JedisCluster> connectMap;
	private Set<HostAndPort> jedisClusterNodes;

	private Set<HostAndPort> getRedisConfig(){
		Set<HostAndPort> jedisClusterNodes = new HashSet<HostAndPort>();
		String redisNodes = ApplicationContext.getConfig("redis.cluster.connect");
		String[] arr = redisNodes.split(",");
		for(String hostport: arr){
			String[] hpArr = hostport.split(":");
			if(hpArr.length==2){
				jedisClusterNodes.add(new HostAndPort(hpArr[0], Integer.parseInt(hpArr[1])));
			}
		}
    	return jedisClusterNodes;
	}
	
	private RedisClusterContext(){
		connectMap = new HashMap<Object,JedisCluster>();
		jedisClusterNodes = getRedisConfig();
	}
	
	
	public JedisCluster getJedisCluster(Object key){
		JedisCluster val = connectMap.get(key);
		if(val==null){
			JedisCluster jc = new JedisCluster(jedisClusterNodes);
			connectMap.put(key, jc);
		}
		return connectMap.get(key);
	}
	
	public void releaseRedisConnector(Object key){
		JedisCluster jc = connectMap.remove(key);
		if(jc!=null){
			try {
				jc.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static RedisClusterContext getInstance(){
		synchronized(RedisClusterContext.class){
			if(redisClusterContext==null){
				redisClusterContext = new RedisClusterContext();
			}
		}
		return redisClusterContext;
	}
	
	
}
