package com.mt;

import com.mt.bolts.WordcounterBolt;
import com.mt.cutomgroup.AlphaGrouping;
import com.mt.spouts.WordReader;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

public class MainApp {

    public static void main(String[] args) {

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("word-reader",new WordReader());
        builder.setBolt("word-counter",new WordcounterBolt(),2).customGrouping("word-reader", new AlphaGrouping());

        Config config = new Config();
        config.put("fileToRead","C:/ShareFolder/sample.txt");
        config.put("dirToWrite","C:/ShareFolder/");
        config.setDebug(true);

        LocalCluster cluster = new LocalCluster();

        try {
            cluster.submitTopology("wordCounter-toplogy",config,builder.createTopology());
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}
