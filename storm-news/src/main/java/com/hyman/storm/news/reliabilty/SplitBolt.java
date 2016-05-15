package com.hyman.storm.news.reliabilty;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class SplitBolt extends BaseRichBolt {

	private static final long serialVersionUID = 2219583090839631011L;
	
	private OutputCollector collector;
	
	@SuppressWarnings("rawtypes")
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
	}

	public void execute(Tuple input) {
		String line = input.getStringByField("line");
		for(String word : line.split(","))
		{
			//注意：走ack机制，必须带上 参数 input emit出去，否则 下一个tuple无法知道原始tuple是什么 从而无法进行ack操作
			//collector.emit(new Values(word)); 不会走ack机制
			collector.emit(input,new Values(word));
			
		}
		collector.ack(input);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

}
