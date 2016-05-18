package com.hyman.storm.drpc;

import org.apache.storm.Config;
import org.apache.storm.thrift.TException;
import org.apache.storm.utils.DRPCClient;
import org.junit.Test;

public class QueryDRPCTest {

	@Test
	public void test1(){
		Config conf = new Config();
		DRPCClient client = null;
		String total = null;
		try {
			client = new DRPCClient(conf, "hyman",3772);
			total = client.execute("exclamation", "msg");
		} catch (TException e) {
			e.printStackTrace();
		}

		System.out.println("The total num of message is : " + total);
	}
	
}
