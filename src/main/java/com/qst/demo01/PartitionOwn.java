package com.qst.demo01;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class PartitionOwn extends Partitioner<Text,NullWritable> {
    @Override
    public int getPartition(Text text, NullWritable nullWritable, int i) {
        String[] split = text.toString().split("\t");
        String gameResult = split[5];
        if(gameResult != null && gameResult != ""){
                if(Integer.parseInt(gameResult) > 15){
                    return 0;
                }else
                {
                    return 1;
                }
        }


        return 0;
    }
}
