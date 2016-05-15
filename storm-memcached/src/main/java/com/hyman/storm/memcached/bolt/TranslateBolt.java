package com.hyman.storm.memcached.bolt;

import java.util.List;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class TranslateBolt extends BaseBasicBolt {
	/**
	 * 
	 */
	private static final long serialVersionUID = 214673588753475836L;

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		List<Object> list= tuple.getValues();
		
		String id = (String) list.get(0);
		String memberid = (String) list.get(1);
		String totalprice = (String) list.get(2);
		String youhui = (String) list.get(3);
		String sendpay = (String) list.get(4);
		
		if("0".equals(sendpay)){
			sendpay = "-1";
		}
		
		System.out.println("list="+list.toString()+"  sendpay = "+sendpay);
		
		collector.emit(new Values(id,memberid,totalprice,youhui,sendpay));

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("id","memberid","totalprice","youhui","sendpay"));

	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
