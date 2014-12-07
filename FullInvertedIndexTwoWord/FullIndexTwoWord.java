package org.myorg;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class FullIndexTwoWord {

  public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    private final static IntWritable one = new IntWritable(1);
    //Create variables to send as (key, value) pair
    private Text word = new Text();
    private Text fileVal = new Text();

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      FileSplit fileSplit = (FileSplit) reporter.getInputSplit();
      String fileName = fileSplit.getPath().getName();
      //The key to be sent is appended with the line offset
      fileVal.set(fileName+"@"+key+", ");

      String line = value.toString();
      //Split string with whitespace
      String[] words = line.split("\\s+");
      //form pairs of two word phrases
      for (int i = 0; i < (words.length-1); i++) {
	if ( !(words[i].equals("")) && !(words[i+1].equals("")) ) {
		word.set(words[i]+" "+words[i+1]+" : ");
		output.collect(word, fileVal);	
	}
      }
    }
  }

  public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      int sum = 0;
      String tmpOut = "";
      String tmpTest = "";
      List<String> LList = new LinkedList<String>();
      //Create a list of the incoming values 
      while (values.hasNext()) {
	tmpOut = values.next().toString();
	if ( !(LList.contains(tmpOut)) && (tmpOut != "")) {
		LList.add(tmpOut);
	}
      }
      //Sort the list
      Collections.sort(LList);
      tmpOut = "";
      ListIterator itr = LList.listIterator(0);
      //Append all the values together to create a new string which is sent out as value
      while (itr.hasNext()) {
	tmpTest = itr.next().toString();
	if (tmpTest != "") 	
		tmpOut += tmpTest; 
      }
      output.collect(key, new Text(""+tmpOut));
    }
  }

  public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf(FullIndexTwoWord.class);
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
