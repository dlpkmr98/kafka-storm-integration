/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.cts.kafkastorm;

import java.util.HashMap;
import java.util.Map;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

/**
 *
 * @author dlpkmr98
 */
public class WordCounterBolt implements IRichBolt {

    Map<String, Integer> counters;
    private OutputCollector collector;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.counters = new HashMap<String, Integer>();
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String str = input.getString(0);
        if (!counters.containsKey(str)) {
            counters.put(str, 1);
        } else {
            Integer c = counters.get(str) + 1;
            counters.put(str, c);
        }

        for (String key : counters.keySet()) {
            System.out.println("*************** [" + key + "," + counters.get(key) + "]");
        }

        collector.ack(input);
    }

    @Override
    public void cleanup() {

        System.out.println("######################################");
        for (String key : counters.keySet()) {
            System.out.println("*************** [" + key + "," + counters.get(key) + "]");
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}
