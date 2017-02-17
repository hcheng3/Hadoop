package org.apache.hadoop.examples;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

public class Query4 {

    public static class Query4Mapper
            extends Mapper<Object, Text, IntWritable, Text> {

        private  Text countryTran = new Text();
        private  IntWritable countryCode = new IntWritable();
        private  Map<Integer, Integer> custCountry = new HashMap<Integer, Integer>();

        public void setup(Context context) throws IOException{
            Path pt = new Path("/user/hadoop/input/customer.csv");  //Location of file in HDFS
            FileSystem fs = FileSystem.get(context.getConfiguration());
            BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
            try{
                String line;
                line = br.readLine();
                System.out.println(line);

                while(line!=null){
                        
                    String[] tuple = line.split(",");
                    
	 	   

                    custCountry.put(Integer.parseInt(tuple[0]), Integer.parseInt(tuple[3]));
                    line =br.readLine();
                }
            }finally{
                br.close();
            }
        }

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] tuple = value.toString().split(",");
            int cId = Integer.parseInt(tuple[1]);  
            countryCode.set(custCountry.get(cId));
            countryTran.set(tuple[2] + "," +tuple[1]);
            context.write(countryCode, countryTran);//key: countrycode value:”TransTotal,CustID”
        }
    }

    public static class Query4Reducer
            extends Reducer<IntWritable, Text, IntWritable, Text> {

        private Text result = new Text();
        private IntWritable outkey = new IntWritable();
        private HashSet<String> custset = new HashSet<String>();

        public void reduce(IntWritable key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {

            float transTotal = 0;
            int custCount = 0;

            float max = 9;
            float min = 1000;

            for(Text value : values){
                String[] tuple = value.toString().split(",");
                if(!custset.contains(tuple[1])){
                    custCount++;
                    custset.add(tuple[1]);
                }
                transTotal = Float.parseFloat(tuple[0]);
                if(transTotal < min){
                    min = transTotal;
                }
                if(transTotal > max){
                    max = transTotal;
                }
            }

            outkey.set(key.get());
            result.set(", "+custCount + ", " + min+", "+max);
            context.write(outkey, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        if (args.length != 2) {
            System.err.println("Usage: query4 <HDFS input file> <HDFS output file>");
            System.exit(2);
        }
        Job job = new Job(conf, "Query4");
        job.setJarByClass(Query4.class);
        job.setMapperClass(Query4Mapper.class);
        job.setReducerClass(Query4Reducer.class);
        job.setNumReduceTasks(2);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
