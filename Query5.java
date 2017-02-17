
package org.apache.hadoop.examples;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Query5{

    private static int customerTotalNum = 50000;
    private static int transTotalNum = 0;

    public static class Query5CustomerMapper
            extends Mapper<Object, Text, Text, Text>{

        private Text customerName = new Text();
        private Text customerID = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String record = value.toString();
            String[] tempArray = record.split(",");
            customerID.set(tempArray[0]);
            customerName.set("c" + "," +tempArray[1]);
            context.write(customerID, customerName);
        }
    }

    public static class Query5TransactionMapper
            extends Mapper<Object, Text, Text, Text>{

        private Text transactionID = new Text();
        private Text customerID = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String record = value.toString();
            String[] tempArray = record.split(",");
            customerID.set(tempArray[1]);
            transactionID.set("t" + "," +tempArray[0]);
            context.write(customerID, transactionID);
        }
    }

    public static class Query5CheckReducer
            extends Reducer<Text, Text , Text , Text>{
        private Text customerName = new Text();
        private Text transactionNum = new Text();

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            String name = "";
            int transSum = 0;

            for (Text val : values) {
                String[] info = val.toString().split(",");
                if (info[0].equals("c")) {
                    name = info[1];
                }else{
                    transSum += 1;
                }
            }
            customerTotalNum += 1;
            transTotalNum += transSum;
            customerName.set(name);
            transactionNum.set(Integer.toString(transSum));
            context.write(customerName, transactionNum);
        }
    }

    public static class Query5NameMapper
            extends Mapper<Object, Text, Text, Text>{
        private Text customer = new Text();
        private Text name = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String record = value.toString();
            String[] tempArray = record.split("\\s+");
            if (Integer.parseInt(tempArray[1]) > (int)(transTotalNum / customerTotalNum)) {
                name.set(tempArray[0]);
                customer.set(record);
                context.write(customer, name);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        boolean ifSuccess = false;
        if (args.length != 4) {
            System.err.println("Usage: query5 <HDFS input file1> <HDFS input file2> <HDFS intermediate file> <HDFS output file>");
            System.exit(2);
        }
        Path input1 = new Path(args[0]);
        Path input2 = new Path(args[1]);
        Path inter = new Path(args[2]);
        Path output = new Path(args[3]);

        ifSuccess = checkTrans(input1, input2, inter);
        if (ifSuccess == false) {
            System.exit(1);
        }
        ifSuccess = findCustomer(inter, output);
        System.exit(ifSuccess ? 0 : 1);
    }

    public static boolean checkTrans(Path pathInput1, Path pathInput2, Path pathTemp) throws Exception{
        Configuration conf = new Configuration();
        Job job = new Job(conf, "query5");
        job.setJarByClass(Query5.class);
        job.setReducerClass(Query5CheckReducer.class);
        job.setNumReduceTasks(4);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job, pathInput1, TextInputFormat.class, Query5CustomerMapper.class);
        MultipleInputs.addInputPath(job, pathInput2, TextInputFormat.class, Query5TransactionMapper.class);
        FileOutputFormat.setOutputPath(job, pathTemp);
        return job.waitForCompletion(true);
    }

    public static boolean findCustomer(Path pathTemp, Path pathOutput) throws Exception{
        Configuration conf = new Configuration();
        Job job = new Job(conf, "query5");
        job.setJarByClass(Query5.class);
        job.setMapperClass(Query5NameMapper.class);
        //job.setReducerClass(Query5NameReducer.class);
        //job.setNumReduceTasks(2);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job, pathTemp);
        FileOutputFormat.setOutputPath(job, pathOutput);
        return job.waitForCompletion(true);
    }

}

