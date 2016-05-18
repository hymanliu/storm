package com.hyman.storm.redis.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertiesUtil {
	
	public static Properties loadConfigraton(){
		return loadConfigraton("application.properties");
	}
	
	public static Properties loadConfigraton(String fileName){
		Properties properties = new Properties();
		InputStream in = PropertiesUtil.class.getClassLoader().getResourceAsStream(fileName);
		try {
			properties.load(in);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return properties;
	}

	
	public static void main(String[] args){
//		System.out.println(ApplicationContext.);
	}
}
