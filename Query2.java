package org.apache.hadoop.examples;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Query2{

    public static class Query2Mapper
            extends Mapper<Object, Text, IntWritable, Text>{

        private Text transTotal = new Text();
        private IntWritable custID = new IntWritable();


        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line= value.toString();
            String[] tuple = line.split(",");

            custID.set(Integer.parseInt(tuple[1]));
            transTotal.set("1"+","+tuple[2]);   //add the one and transSum
            context.write(custID, transTotal);

        }
    }

    public static class Query2Reducer
            extends Reducer<IntWritable, Text , IntWritable , Text>{
        private Text result = new Text();

        public void reduce(IntWritable key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            String tuple[];
            float total = 0;
            int count = 0;

            for (Text value : values) {
                tuple = value.toString().split(",");
                total += Float.parseFloat(tuple[1]);
                count += Integer.parseInt(tuple[0]);
            }

            result.set(count +","+total);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        if (args.length != 2) {
            System.err.println("Usage: query2 <HDFS input file> <HDFS output file>");
            System.exit(2);
        }

        Job job = new Job(conf, "query2");
        job.setJarByClass(Query2.class);
        job.setMapperClass(Query2Mapper.class);
        job.setCombinerClass(Query2Reducer.class);
        job.setReducerClass(Query2Reducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setNumReduceTasks(2);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

