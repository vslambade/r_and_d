package com.mt.bolts;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Map;

public class SlidingWindowBolt extends BaseWindowedBolt  {

    private OutputCollector collector;
    private PrintWriter writer;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        String fileName = stormConf.get("fileToWrite").toString();
        try {
            this.writer = new PrintWriter(fileName, "UTF-8");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

    public void execute(TupleWindow tupleWindow) {

        String symbol = null;
        String timeStamp = null;
        Double price =0.0 ,prevClose = 0.0 ,priceResult = 0.0,prevCloseResult=0.0;

        //do th computation over here
        for(Tuple tuple : tupleWindow.get()){

            symbol = tuple.getString(0);
            timeStamp = tuple.getString(1);
            price = tuple.getDoubleByField("price");
            prevClose = tuple.getDoubleByField("prev_close");

            //computation part
            priceResult += price;
            prevCloseResult +=prevClose;

        }

        //emits the reuslt after computation
        collector.emit(new Values(symbol,timeStamp,price,prevClose));
        writer.println(symbol+" "+timeStamp+"  total ="+priceResult+"  prev total = "+prevCloseResult);
    }

    @Override
    public void cleanup() {
        writer.close();
    }


}
