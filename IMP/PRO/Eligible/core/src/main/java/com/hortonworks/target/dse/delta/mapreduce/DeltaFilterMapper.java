package com.hortonworks.target.dse.delta.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DeltaFilterMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private static final IntWritable oneWritable = new IntWritable(1);

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        context.write(value, oneWritable);
    }
}
