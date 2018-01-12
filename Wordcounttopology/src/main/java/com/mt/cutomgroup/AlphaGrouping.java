package com.mt.cutomgroup;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class AlphaGrouping implements CustomStreamGrouping,Serializable {

    private List<Integer> targetTasks;

    public void prepare(WorkerTopologyContext workerTopologyContext, GlobalStreamId globalStreamId, List<Integer> list) {
        this.targetTasks = list;
    }

    public List<Integer> chooseTasks(int i, List<Object> list) {

        List<Integer> boltsIds = new ArrayList<Integer>();

        String word = list.get(0).toString();

        if(word.startsWith("a")){
            boltsIds.add(targetTasks.get(0));
        }else{
            boltsIds.add(targetTasks.get(1));
        }

        return null;
    }
}
