package com.mt;

import com.mt.bolts.SlidingWindowBolt;
import com.mt.spouts.YfsSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;

import java.util.concurrent.TimeUnit;

public class WindowingToplogy  {
    public static void main(String[] args) {

        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("spout",new YfsSpout());
        topologyBuilder.setBolt("slidingwindowbolt",new SlidingWindowBolt().withWindow(new BaseWindowedBolt.Count(10),new BaseWindowedBolt.Duration(5000, TimeUnit.MILLISECONDS))).shuffleGrouping("spout");

        StormTopology topology = topologyBuilder.createTopology();

        Config config = new Config();
        config.setDebug(true);
        config.put("fileToWrite","D:/testdocs/windowing-output.txt");

        //Submit Topoloyt to cluster
        LocalCluster cluster = new LocalCluster();

        try {
            cluster.submitTopology("Stock-Tracker-Topology",config,topology);
            Thread.sleep(50000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }finally {
            cluster.shutdown();
        }

    }
}
