package com.hyman.storm.news.reliabilty;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

public class ExportBolt extends BaseRichBolt {

	private static final long serialVersionUID = -5583439069401101320L;
	
	private OutputCollector collector;
	private FileWriter writer;
	int count = 0;

	@SuppressWarnings("rawtypes")
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		try {
			writer = new FileWriter("E:/exportbolt"+this+".txt");
		} catch (IOException e) {
		}
	}

	public void execute(Tuple input) {
		try {
			if(count==5) throw new RuntimeException("Tuple process fail...");
			String word = input.getStringByField("word");
			writer.write(word);
			writer.write("\n");
			writer.flush();
			collector.ack(input);
			
		} catch (Exception e) {
			e.printStackTrace();
			collector.fail(input);
		}
		count++;
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	@Override
	public void cleanup() {
		try {
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	
}
