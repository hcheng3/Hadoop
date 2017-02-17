package org.apache.hadoop.examples;
import java.lang.*;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Query3{

    public static class Query3CustomerMapper
            extends Mapper<Object, Text, Text, Text>{

        private Text infoArray = new Text();
        private Text customerID = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String record = value.toString();
            String[] tempArray = record.split(",");
            customerID.set(tempArray[0]);
            infoArray.set("c" + "," + tempArray[1] + "," + tempArray[4]);
            context.write(customerID, infoArray);
        }
    }

    public static class Query3TransactionMapper
            extends Mapper<Object, Text, Text, Text>{

        private Text infoArray = new Text();
        private Text customerID = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String record = value.toString();
            String[] tempArray = record.split(",");
            customerID.set(tempArray[1]);
            infoArray.set("t" + "," + tempArray[2] + "," + tempArray[3]);
            context.write(customerID, infoArray);
        }
    }

    public static class Query3Reducer
            extends Reducer<Text, Text, Text , Text>{
        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            float totalSum = 0;
            int numTrans = 0;
            int minItemNum = 11;
            String name = "";
            String salary = "";

            for (Text val : values) {
                String[] info = val.toString().split(",");
                if (info[0].equals("c")) {
                    name = info[1];
                    salary = info[2];
                }else{
                    totalSum += Float.parseFloat(info[1]);
                    numTrans += 1;
                    minItemNum = Math.min(minItemNum, Integer.parseInt(info[2]));
                }
            }
            result.set(key.toString() + "," + name + "," + salary + "," + numTrans + "," + totalSum + "," + minItemNum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        if (args.length != 3) {
            System.err.println("Usage: query3 <HDFS input file1> <HDFS input file2> <HDFS output file>");
            System.exit(2);
        }
        Job job = new Job(conf, "query3");
        job.setJarByClass(Query3.class);
        job.setReducerClass(Query3Reducer.class);
        job.setNumReduceTasks(8);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, Query3CustomerMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, Query3TransactionMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

