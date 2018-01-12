package com.mt;

import com.mt.functions.Count;
import com.mt.functions.SplitFunction;
import com.mt.spouts.WordReader;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.tuple.Fields;

import java.awt.peer.TrayIconPeer;

public class MainApp {

    public static void main(String[] args) {

        TridentTopology tridentTopology = new TridentTopology();
        tridentTopology.newStream("lines",new WordReader())
                .each(new Fields("word"),new SplitFunction(),new Fields("word_split"))
                .groupBy(new Fields("word_split"))
                //.aggregate(new Count(),new Fields("count"))
                .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count")).newValuesStream();
               // .each(new Fields("word_split"),new Debug());

        Config config = new Config();
        config.setDebug(true);
        config.put("fileToRead","C:/ShareFolder/sample.txt");

        LocalCluster cluster = new LocalCluster();

        try {
            cluster.submitTopology("trident-toplogy",config,tridentTopology.build());
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }finally {
            cluster.shutdown();
        }

    }
}
