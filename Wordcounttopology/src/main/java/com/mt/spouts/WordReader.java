package com.mt.spouts;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

public class WordReader extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private FileReader fileReader;
    private BufferedReader reader;

    private boolean completed = false;

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;

        try
        {
            this.fileReader = new FileReader(map.get("fileToRead").toString());
        }catch (Exception e){
            throw  new RuntimeException("Error reading file ...");
        }

        this.reader = new BufferedReader(fileReader);


    }

    public void nextTuple() {

        if(!completed)
            try {
            String word = reader.readLine();

            if (word != null) {
                word = word.trim();
                word = word.toLowerCase();
                //emitting the word
                collector.emit(new Values(word));
            } else {
                completed = true;
                fileReader.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word"));
    }
}
