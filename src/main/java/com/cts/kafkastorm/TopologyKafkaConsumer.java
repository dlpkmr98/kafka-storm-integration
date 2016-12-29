/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.cts.kafkastorm;

import storm.kafka.bolt.KafkaBolt;
import storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import storm.kafka.bolt.selector.DefaultTopicSelector;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import storm.trident.testing.FixedBatchSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;


/**
 *
 * @author dlpkmr98
 */
public class TopologyKafkaConsumer {

    public static void main(String[] args) throws InterruptedException {

        TopologyBuilder builder = new TopologyBuilder();
        Fields fields = new Fields("key", "message");
        FixedBatchSpout spout = new FixedBatchSpout(fields,
                4,
                new Values("storm", "1"),
                new Values("trident", "1"),
                new Values("needs", "1"),
                new Values("javadoc", "1")
        );
        spout.setCycle(true);
        builder.setSpout("spout", (IRichSpout) spout, 5);      
        KafkaBolt bolt = new KafkaBolt()    
                     .withTopicSelector(new DefaultTopicSelector("test"))
                     .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper());
        builder.setBolt("forwardToKafka", bolt, 8).shuffleGrouping("spout");
        
//        Config conf = new Config();  
//    //set producer properties.
//    Properties props = new Properties();
//    props.put("metadata.broker.list", "localhost:9092");
//    props.put("request.required.acks", "1");
//    props.put("serializer.class", "kafka.serializer.StringEncoder");
//    conf.put(KafkaBolt.KAFKA_BROKER_PROPERTIES, props);
        
    }

}
