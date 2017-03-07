package com.hortonworks.target.dse.delta.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class DeltaFilterMapperReducerTest {
    MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
    ReduceDriver<Text, IntWritable, Text, NullWritable> reduceDriver;
    MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, NullWritable> mapReduceDriver;

    @Before
    public void setUp() {
        DeltaFilterMapper mapper = new DeltaFilterMapper();
        DeltaFilterReducer reducer = new DeltaFilterReducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(), new Text("000020000100431000032015-03-229999-12-31"));
        mapDriver.withOutput(new Text("000020000100431000032015-03-229999-12-31"), new IntWritable(1));
        mapDriver.runTest(false);
    }

    @Test
    public void testReducer() throws IOException {
        List<IntWritable> values = new ArrayList<IntWritable>();
        values.add(new IntWritable(1));
        values.add(new IntWritable(1));
        reduceDriver.withInput(new Text("000020000100431000032015-03-229999-12-31"), values);
        reduceDriver.runTest(false);
    }

    @Test
    public void testMapReduce() throws IOException {
        mapReduceDriver.addInput(new LongWritable(), new Text("000020000100431000032015-03-229999-12-31")); // 1
        mapReduceDriver.addInput(new LongWritable(), new Text("000020000100434000032015-03-229999-12-31")); // 2
        mapReduceDriver.addInput(new LongWritable(), new Text("000020000100431000032015-03-229999-12-31")); // Copy of 1
        mapReduceDriver.addInput(new LongWritable(), new Text("000020000100433000032015-03-229999-12-31")); // 3
        mapReduceDriver.addInput(new LongWritable(), new Text("000020000100433000032015-03-229999-12-31")); // Copy of 3
        mapReduceDriver.addInput(new LongWritable(), new Text("000020000100433000032015-03-229999-12-31")); // Copy of 3
        mapReduceDriver.addInput(new LongWritable(), new Text("000020000100433000032015-03-229999-12-30")); // 4

        mapReduceDriver.withOutput(new Text("000020000100434000032015-03-229999-12-31"), NullWritable.get());
        mapReduceDriver.withOutput(new Text("000020000100433000032015-03-229999-12-30"), NullWritable.get());

        mapReduceDriver.runTest(false);
    }
}
