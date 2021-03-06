package com.storm.reliability.spout;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class MessageSpout extends BaseRichSpout {

	private static final long serialVersionUID = -4664068313075450186L;

	private int index = 0;
	
	private String[] lines;
	
	private SpoutOutputCollector collector;
	
	public MessageSpout(){
		lines = new String[]{
				"0,zero",
				"1,one",
				"2,two",
				"3,three",
				"4,four",
				"5,five",
				"6,six",
				"7,seven",
				"8,eight",
				"9,nine"
		};
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("line"));
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}
	
	@Override
	public void nextTuple() {
		if(index < lines.length){
			String l = lines[index];
			collector.emit(new Values(l), "msgid-"+index);
			index++;
		}
	}

	@Override
	public void ack(Object msgId) {
		System.out.println("message sends successfully (msgId = " + msgId +")");
	}

	@Override
	public void fail(Object msgId) {
		System.out.println("error : message sends unsuccessfully (msgId = " + msgId +")");
		System.out.println("resending...");
		String[] arr = msgId.toString().split("-");
		collector.emit(new Values(lines[Integer.parseInt(arr[1])]), msgId);
		System.out.println("resend successfully");
	}

}
