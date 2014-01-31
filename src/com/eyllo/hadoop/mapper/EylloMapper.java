package com.eyllo.hadoop.mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.eyllo.hadoop.EylloJobLauncher;

public class EylloMapper extends Mapper<Object, Text, Text, Text>{

  private static Map<String, String> stops = new HashMap<String, String> ();

  @Override
  public void setup(Context context) {
    String stopPath = context.getConfiguration().get(EylloJobLauncher.STOPS_PATH);
    if (stopPath != null) {
      Configuration conf = context.getConfiguration();
      Path path = new Path(stopPath);
      try {
        FileSystem fs = FileSystem.get(path.toUri(), conf);
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
        String line;
        line=br.readLine();
        line=br.readLine();
        String stopId;
        while (line != null){
                //System.out.println(line);
                StringTokenizer itr = new StringTokenizer(line, ",");
                stopId = itr.nextElement().toString();
                stops.put(stopId, "");
                line=br.readLine();
        }
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
      
  }

  public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    String [] itr = value.toString().split(",");
    if (itr.length == 5) {
      StringBuilder strBuilder = new StringBuilder();
      //System.out.println(value.toString());
      if (!value.toString().contains("trip_id")) {
        if (stops.get(itr[4]) != null && !stops.get(itr[4]).toString().equals("")) 
          strBuilder.append(stops.get(itr[4]).toString());
        strBuilder.append("Trip:").append(itr[0]);
        strBuilder.append("Arrives @:").append(itr[1]);
        strBuilder.append("Departs @:").append(itr[2]);
        strBuilder.append("\n");
        stops.put(itr[4], strBuilder.toString());
      }
    }
  }

  @Override
  public void cleanup(Context context) {
    Text key = new Text();
    Text value = new Text();
    try {
      for (Entry<String, String> entr : stops.entrySet()) {
        key.set(entr.getKey());
        value.set(entr.getValue());
        if (!entr.getValue().isEmpty())
          context.write(key, value);
      }
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

}
