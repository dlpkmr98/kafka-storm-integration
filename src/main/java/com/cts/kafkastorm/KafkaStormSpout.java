/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.cts.kafkastorm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import java.util.Properties;
import java.util.UUID;


import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;    
import storm.kafka.KeyValueSchemeAsMultiScheme;
import storm.kafka.SpoutConfig;

import storm.kafka.ZkHosts;   
import storm.kafka.bolt.KafkaBolt;

import storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import storm.kafka.bolt.selector.DefaultTopicSelector;



/**
 *
 * @author DILIP
 */
public class KafkaStormSpout {
    
    


    public static void main(String[] args) {

        String zkConnString = "localhost:2181";
        String topicName = "filedata";
        BrokerHosts hosts = new ZkHosts(zkConnString);
        SpoutConfig spoutConfig = new SpoutConfig(hosts, topicName, "/" + topicName, UUID.randomUUID().toString());
       // spoutConfig.bufferSizeBytes = 1024 * 1024 * 4;
       // spoutConfig.fetchSizeBytes = 1024 * 1024 * 4;
        
        spoutConfig.scheme = new KeyValueSchemeAsMultiScheme(new KafkaBoltKeyValueScheme());  

        //spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", kafkaSpout, 5);
        KafkaBolt bolt = new KafkaBolt()
                .withTopicSelector(new DefaultTopicSelector(topicName))
                .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper());    
                
        builder.setBolt("forwardToKafka", bolt, 8).shuffleGrouping("spout");  
        Config conf = new Config();
        conf.setDebug(true);
        //set producer properties.
        Properties props = new Properties();
        props.put("metadata.broker.list", "localhost:9092");
        props.put("bootstrap.servers", "localhost:9092");
        props.put("request.required.acks", "1");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("key.serializer", "kafka.serializer.StringEncoder");
        props.put("value.serializer", "kafka.serializer.StringEncoder");
        conf.put(KafkaBolt.KAFKA_BROKER_PROPERTIES, props);        
           
        try {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("HelloStorm", conf, builder.createTopology());
        } catch (Exception e) {
            throw new IllegalStateException("Couldn't initialize the topology", e);
        }

    }

}



