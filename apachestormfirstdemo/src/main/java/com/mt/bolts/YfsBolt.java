package com.mt.bolts;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Map;

/**
 * User: vidhy
 * Date: 1/8/2018
 * Project Name : apachestormfirstdemo
 */
public class YfsBolt extends BaseBasicBolt {

    private PrintWriter writer;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {

        String fileName = stormConf.get("fileToWrite").toString();

        try {
            this.writer = new PrintWriter(fileName, "UTF-8");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {

        String symbol = tuple.getString(0);
        String timeStamp = tuple.getString(1);
        Double price = tuple.getDoubleByField("price");
        Double prevClose = tuple.getDoubleByField("prev_close");

        basicOutputCollector.emit(new Values(symbol,timeStamp,price,prevClose));
        writer.println(symbol+" "+timeStamp+"  "+price+"  "+prevClose);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("company","timestamp","price","prev_close"));
    }

    @Override
    public void cleanup() {
        writer.close();
    }
}
