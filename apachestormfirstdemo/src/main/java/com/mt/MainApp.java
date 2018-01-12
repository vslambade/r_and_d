package com.mt;

import com.mt.bolts.YfsBolt;
import com.mt.spouts.YfsSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

/**
 * User: vidhy
 * Date: 1/8/2018
 * Project Name : apachestormfirstdemo
 */
public class MainApp {

    public static void main(String[] args) {

        //Build Topology
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("Yahoo-finance-spout", new YfsSpout());
        topologyBuilder.setBolt("Yahoo-finance-bolt",new YfsBolt()).shuffleGrouping("Yahoo-finance-spout");

        StormTopology topology = topologyBuilder.createTopology();

        //configuration
        Config config = new Config();
        config.setDebug(true);
        config.put("fileToWrite","D:/testdocs/output.txt");

        //Submit Topoloyt to cluster
        LocalCluster cluster = new LocalCluster();

        try {
            cluster.submitTopology("Stock-Tracker-Topology",config,topology);
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }finally {
            cluster.shutdown();
        }


    }
}
