package com.storm.drcp;


import backtype.storm.utils.DRPCClient;

public class ClientTest {

	public static void main(String[] args) throws Exception {
		
		DRPCClient client = new DRPCClient("hyman1",3772);

		for (String word : new String[]{ "hello", "world" }) {
			System.out.println("Result for \"" + word + "\": " + client.execute("exclamation", word));
		}
	}

}
