/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.cts.kafkastorm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

/**
 *
 * @author dlpkmr98
 */
public class WordCountTopology {
    
    public static void main(String[] args) throws InterruptedException {
        
        Config config = new Config();
        config.put("inputFile", "C:\\Users\\dlpkmr98\\Desktop\\person.txt");
        config.setDebug(true);
        config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("line-reader-spout", new LineReaderSpout());
        builder.setBolt("word-spitter", new WordSpitterBolt()).shuffleGrouping("line-reader-spout");
        builder.setBolt("word-counter", new WordCounterBolt()).shuffleGrouping("word-spitter");
        
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("HelloStorm", config, builder.createTopology());
        
        //System.out.println("+++++++++"+cluster.getClusterInfo());
        Thread.sleep(10000);
       cluster.shutdown();
    }
    
}
