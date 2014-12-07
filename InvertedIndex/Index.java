package org.myorg;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Index {

  public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    private final static IntWritable one = new IntWritable(1);

    //Create Text variables which will be sent out as (key, value) pairs
    private Text word = new Text();
    private Text fileVal = new Text();

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      FileSplit fileSplit = (FileSplit) reporter.getInputSplit();
      String fileName = fileSplit.getPath().getName();
      fileVal.set(fileName+", ");

      String line = value.toString();
      StringTokenizer tokenizer = new StringTokenizer(line);
      while (tokenizer.hasMoreTokens()) {
        word.set(tokenizer.nextToken());
	//The final (key, value) pair should look like ("<word> :", "filename, ")
	output.collect(new Text(word.toString()+" : "), fileVal);
      }
    }
  }

  public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      int sum = 0;
      String tmpOut = "";
      String tmpTest = "";
      List<String> LList = new LinkedList<String>();
      //Read in all the filenames for a word in a List. This simplifies sorting.
      while (values.hasNext()) {
	tmpOut = values.next().toString();
	if ( !(LList.contains(tmpOut)) && (tmpOut != "")) {
		LList.add(tmpOut);
	}
      }
	
      //Sort the collect list of values
      Collections.sort(LList);
      tmpOut = "";
      ListIterator itr = LList.listIterator(0);
      //Create a new Text string
      while (itr.hasNext()) {
	tmpTest = itr.next().toString();
	if (tmpTest != "") 	
		tmpOut += tmpTest; 
      }
      //Send out the (key, value) pair
      output.collect(key, new Text(""+tmpOut));
    }
  }

  public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf(Index.class);
    conf.setJobName("wordcount");

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);

    conf.setMapperClass(Map.class);
    conf.setCombinerClass(Reduce.class);
    conf.setReducerClass(Reduce.class);

    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);

    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

    JobClient.runJob(conf);
  }
}
