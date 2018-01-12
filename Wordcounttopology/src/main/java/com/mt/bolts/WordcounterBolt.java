package com.mt.bolts;

import org.apache.storm.task.IBolt;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

public class WordcounterBolt extends BaseBasicBolt implements IBasicBolt {

    private Map<String,Integer> counters;
    private Integer id;
    private String name;
    private String fileName;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {

        this.counters = new HashMap<String, Integer>();

        this.name = context.getThisComponentId();
        this.id = context.getThisTaskId();
        this.fileName = stormConf.get("dirToWrite").toString()+"output-"+name+id+".txt";

    }

    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {

        String word = tuple.getString(0);
        if(!counters.containsKey(word)){
            counters.put(word,1);
        }else{
            Integer c = counters.get(word) + 1;
            counters.put(word,1);
        }
    }

    @Override
    public void cleanup() {

        System.out.println("Inside the cleanup method . . . . . ");

        try{
            PrintWriter writer = new PrintWriter(fileName,"UTF-8");

            for(Map.Entry<String , Integer> entry : counters.entrySet()){
                writer.println(entry.getKey()+": "+entry.getValue());
                System.out.println("wirter wirte: "+entry.getKey()+": "+entry.getValue());
            }

            writer.close();

        }catch (Exception e){
            throw  new RuntimeException();
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word"));
    }
}
