package org.apache.hadoop.examples;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Query1 {

    public static class Query1Mapper
            extends Mapper<Object, Text, Text, Text>{

        private Text record = new Text();
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String tempStr = value.toString();
            String[] tempArray = tempStr.split(",");
            Integer countryCode = Integer.parseInt(tempArray[3]);
            if (countryCode >= 2 && countryCode <= 6) {
                record.set(tempStr);
                word.set(tempArray[0]);
                context.write(word, record);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        if (args.length != 2) {
            System.err.println("Usage: query1 <HDFS input file> <HDFS output file>");
            System.exit(2);
        }
        Job job = new Job(conf, "query1");
        job.setJarByClass(Query1.class);
        job.setMapperClass(Query1Mapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}