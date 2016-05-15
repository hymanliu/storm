package com.hyman.storm.news.mysql;

import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.jdbc.bolt.JdbcInsertBolt;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.HikariCPConnectionProvider;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcMapper;
import org.apache.storm.shade.com.google.common.collect.Maps;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

import com.hyman.storm.news.reliabilty.MessageSpout;
import com.hyman.storm.news.reliabilty.SplitBolt;

public class MainTopology {
	
	static final String MESSAGE_SPOUT = "message-spout";
	static final String SPLIT_BOLT = "split-bolt";
	static final String SAVE_BOLT = "save-bolt";

	public static void main(String[] args) throws Exception {
		
		
		Map<String, Object> hikariConfigMap = Maps.newHashMap();
		hikariConfigMap.put("dataSourceClassName","com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
		hikariConfigMap.put("dataSource.url", "jdbc:mysql://localhost/test");
		hikariConfigMap.put("dataSource.user","root");
		hikariConfigMap.put("dataSource.password","123456");
		ConnectionProvider connectionProvider = new HikariCPConnectionProvider(hikariConfigMap);

		String tableName = "t_word";
		JdbcMapper simpleJdbcMapper = new SimpleJdbcMapper(tableName, connectionProvider);
		
		JdbcInsertBolt userPersistanceBolt = new JdbcInsertBolt(connectionProvider, simpleJdbcMapper)
        .withInsertQuery("insert into t_word values (?)")
        .withQueryTimeoutSecs(30);   
		
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout(MESSAGE_SPOUT, new MessageSpout(), 1).setNumTasks(1);
		builder.setBolt(SPLIT_BOLT, new SplitBolt(),1).setNumTasks(1).shuffleGrouping(MESSAGE_SPOUT);
		builder.setBolt(SAVE_BOLT, userPersistanceBolt,1).shuffleGrouping(SPLIT_BOLT);
		
		Config conf = new Config();
		
		conf.setNumAckers(1);
		conf.setDebug(false);
		if(args!=null && args.length > 0) {
			conf.setNumWorkers(2);
			StormSubmitter.submitTopology(args[0], conf,builder.createTopology());
		} else {
	        LocalCluster cluster = new LocalCluster();
	        cluster.submitTopology("mysql-topology", conf, builder.createTopology());
//	        Utils.sleep(60000);
//	        cluster.killTopology("mysql-topology");
//	        cluster.shutdown();
//	        System.exit(0);
		}
		
		
	}

}
