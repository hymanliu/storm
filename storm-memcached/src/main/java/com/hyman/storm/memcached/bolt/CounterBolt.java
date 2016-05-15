package com.hyman.storm.memcached.bolt;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

public class CounterBolt extends BaseBasicBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5508421065181891596L;
	
	private static long counter = 0;
	
	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		
		System.out.println("msg = "+tuple.getString(0)+" -------------counter = "+(counter++));

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}

}
