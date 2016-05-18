package com.hyman.storm.redis.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ApplicationContext {
	private static ApplicationContext context;
	private Map<String,String> config;
	private ApplicationContext(){
		config = new HashMap<String,String>();
		Properties prop = PropertiesUtil.loadConfigraton();
		for(Map.Entry<Object,Object> entry : prop.entrySet()){
			String key = (String) entry.getKey();
			String val = (String) entry.getValue();
			config.put(key, val);
		}
	}
	
	public String getProperty(String key){
		return config.get(key);
	}
	
	static {
		context = new ApplicationContext();
	}
	
	public static String getConfig(String key){
		return context.getProperty(key);
	}
}
