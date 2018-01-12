import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.Consumer;
import org.apache.storm.trident.testing.CountAsAggregator;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.testing.Split;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.trident.windowing.WindowsStoreFactory;
import org.apache.storm.trident.windowing.config.TumblingCountWindow;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

public class TridentHBaseWindowingStoreTopology {

    private static final Logger LOG = LoggerFactory.getLogger(TridentHBaseWindowingStoreTopology.class);

    public static StormTopology buildTopology(WindowsStoreFactory windowsStore) throws Exception {

        FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 3, new Values("the cow jumped over the moon"),
                new Values("the man went to the store and bought some candy"), new Values("four score and seven years ago"),
                new Values("how many apples can you eat"), new Values("to be or not to be the person"));
        spout.setCycle(true);

        TridentTopology topology = new TridentTopology();

        Stream stream = topology.newStream("spout1", spout).parallelismHint(16).each(new Fields("sentence"),
                new Split(), new Fields("word"))
                .window(TumblingCountWindow.of(1000), windowsStore, new Fields("word"), new CountAsAggregator(), new Fields("count"))
                .peek(new Consumer() {
                    public void accept(TridentTuple input) {
                        LOG.info("Received tuple: [{}]", input);
                    }
                });

        return topology.build();
    }

    public static void main(String[] args) throws Exception {

        Config conf = new Config();
        conf.setMaxSpoutPending(20);
        conf.put(Config.TOPOLOGY_TRIDENT_WINDOWING_INMEMORY_CACHE_LIMIT, 100);

        // window-state table should already be created with cf:tuples column
        HBaseWindowsStoreFactory windowStoreFactory = new HBaseWindowsStoreFactory(new HashMap<String, Object>(), "window-state", "cf".getBytes("UTF-8"), "tuples".getBytes("UTF-8"));
    }

}
