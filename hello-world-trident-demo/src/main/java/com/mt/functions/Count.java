package com.mt.functions;

import org.apache.storm.trident.operation.CombinerAggregator;
import org.apache.storm.trident.tuple.TridentTuple;

public class Count implements CombinerAggregator<Long> {
    public Long init(TridentTuple tridentTuple) {
        return 1l;
    }

    public Long combine(Long aLong, Long t1) {
        return aLong + t1;
    }

    public Long zero() {
        return 0l;
    }
}
