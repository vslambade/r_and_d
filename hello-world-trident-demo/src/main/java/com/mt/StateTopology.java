package com.mt;

import com.mt.functions.SplitFunction;
import com.mt.spouts.WordReader;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.Debug;
import org.apache.storm.trident.operation.builtin.MapGet;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.tuple.Fields;

public class StateTopology {
    public static void main(String[] args) {
        TridentTopology tridentTopology = new TridentTopology();
        TridentState wordCounts =  tridentTopology.newStream("lines",new WordReader())
                .each(new Fields("word"),new SplitFunction(),new Fields("word_split"))
                .groupBy(new Fields("word_split"))
                .persistentAggregate(new MemoryMapState.Factory(),new Count(),new Fields("count"));

        LocalDRPC drpc = new LocalDRPC();
        tridentTopology.newDRPCStream("lines",drpc)
                .stateQuery(wordCounts,new Fields("args"),new MapGet(),new Fields("count"));

        Config config = new Config();
        config.setDebug(true);
        config.put("fileToRead","C:/ShareFolder/sample.txt");

        LocalCluster cluster = new LocalCluster();

        try {
            cluster.submitTopology("trident-toplogy",config,tridentTopology.build());

            for(String word : new String[]{"very" ,"very" ,"short","boot","this", "is"," a", "very"," very", "long", "book"}){
                System.out.println("Result For"+ word + " "+drpc.execute("lines",word));
            }

            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }finally {
            cluster.shutdown();
        }

    }
}
