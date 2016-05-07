package com.hyman.storm.example;

import java.util.Map;

import com.hyman.storm.utils.Utils;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class SentenceSpout extends BaseRichSpout {

	private static final long serialVersionUID = -5575171622945098879L;

	private SpoutOutputCollector collector;
	
	private String[] sentences = {
	        "Hello world this is a storm example",
	        "storm is a realtime tools",
	        "the dog ate my homework",
	        "don't have a cow man",
	        "i don't think i like fleas"
	    };
	
	private int index = 0;

	@Override
	@SuppressWarnings("rawtypes")
	public void open(Map conf, TopologyContext context,SpoutOutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void nextTuple() {
		this.collector.emit(new Values(sentences[index % sentences.length]));
        index++;
        Utils.waitForMillis(1);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sentence"));
	}

}
