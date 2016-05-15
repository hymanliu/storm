package com.hyman.storm.news.reliabilty;

import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

public class MessageSpout extends BaseRichSpout {

	private static final long serialVersionUID = 2336886084325696371L;
	private int index = 0;
	
	private static String[] lines = new String[]{
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
	
	private SpoutOutputCollector collector;

	@SuppressWarnings("rawtypes")
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector=collector;
	}
	
	public void nextTuple() {
		Utils.sleep(100);
		if(index < lines.length){
			collector.emit(new Values(lines[index]), "msgid-"+index);
		}
		index++;
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

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("line"));
	}
	

}
