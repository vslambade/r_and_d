package com.mt.spouts;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import yahoofinance.YahooFinance;
import yahoofinance.quotes.stock.StockQuote;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Map;

/**
 * User: vidhy
 * Date: 1/8/2018
 * Project Name : apachestormfirstdemo
 */
public class YfsSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss");

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
    }

    public void nextTuple() {

        try {
            StockQuote quote = YahooFinance.get("MSFT").getQuote();

            BigDecimal price = quote.getPrice();
            BigDecimal prevClose = quote.getPreviousClose();
            Timestamp timestamp = new Timestamp(System.currentTimeMillis());

            collector.emit(new Values("MSFT",simpleDateFormat.format(timestamp),price.doubleValue(),prevClose.doubleValue()));

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("company","timestamp","price","prev_close"));
    }
}
