package com.hortonworks.target.dse.delta.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class DeltaFilterReducer extends Reducer<Text, IntWritable, Text, NullWritable> {
    private static final NullWritable nullWritable = NullWritable.get();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //Discard the first value
        Iterator<IntWritable> iter = values.iterator();
        iter.next();
        // If we do not have another value, then we have a row unique to one of the input and output.
        // If we do, output nothing because there is no delta to be accounted for.
        if(!iter.hasNext()) {
            context.write(key, nullWritable);
        }
    }
}
