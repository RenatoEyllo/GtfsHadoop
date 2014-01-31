package com.eyllo.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.eyllo.hadoop.mapper.EylloMapper;

public class EylloJobLauncher {

  /** Parameter where the stops path will go. */
  public static final String STOPS_PATH = "stops";
  public static final String LOCAL_EXEC = "local";

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    if (args.length != 3) {
      System.err.println("Usage: <stops> <stop_times> <out>");
      System.exit(2);
    }
    conf.set(STOPS_PATH, args[0]);
    
    Job job = new Job(conf, "Eyllo Job");
    job.setJarByClass(EylloJobLauncher.class);
    job.setMapperClass(EylloMapper.class);
    //job.setCombinerClass(EylloReducer.class);
    //job.setReducerClass(EylloReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[1]));
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
