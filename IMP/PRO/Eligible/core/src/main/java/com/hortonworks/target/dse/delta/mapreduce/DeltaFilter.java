package com.hortonworks.target.dse.delta.mapreduce;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class DeltaFilter extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int result = ToolRunner.run(new Configuration(), new DeltaFilter(), args);
        System.exit(result);
    }

    @Override
    public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf);

        // Output Types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // Job Classes
        job.setMapperClass(DeltaFilterMapper.class);
        job.setReducerClass(DeltaFilterReducer.class);

        //String[] paths = args[0].split(",");
        String latestPath = conf.get("latestPath");
        String previousPath = conf.get("previousPath");
        System.out.println("Path 0 " + latestPath);
        System.out.println("Path 1 " + previousPath);

        MultipleInputs.addInputPath(job, new Path(latestPath), TextInputFormat.class, DeltaFilterMapper.class);
        MultipleInputs.addInputPath(job, new Path(previousPath), TextInputFormat.class, DeltaFilterMapper.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(conf.get("outputPath")));

        job.setJarByClass(DeltaFilter.class);

        boolean result = job.waitForCompletion(true);
        return (result ? 0 : 1);
    }
}
