package com.mt.functions;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

public class SplitFunction extends BaseFunction {
    public void execute(TridentTuple tuple, TridentCollector collector) {
        String sentence = tuple.getString(0);
        String[] words = sentence.split(" ");

        for (String word:words) {
            word = word.trim();

            if(word.length()!=0){
                System.out.println("Emmiting the word . . . "+word);
                collector.emit(new Values(word));
            }
        }

    }
}
