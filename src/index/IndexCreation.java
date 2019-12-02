package index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;

public class IndexCreation {
    public static class AgeMapper
            extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            ///TODO: Emit {splitID+attribute, record} - where splitID is the split_start and record contains offset

            String splitId = getSplitID(context);
            System.out.println("++++++++++++++++++++++++++++++=" + splitId);

            String[] attrs = value.toString().split(",");
            String attribute = attrs[2];

            context.write(new Text(splitId + "+" + attribute), value);

            //context.write(new Text(custid), new Text(data));
        }

        public String getSplitID(Context context) {
            return context.getInputSplit().toString();
        }
    }


    public static class TransactionsReducer
            extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // TODO: Fix offset- not sure if it will be the splitID (in key)  or added into the value
            HashMap trojanIndex = new HashMap<String, Integer> ();
            for (Text v: values) {
                String[] keys = key.toString().split("[+]");
                String offset = keys[1];
                String[] attrs = v.toString().split(",");
                String value = attrs[2];
                if (!trojanIndex.containsKey(value)) {
                    trojanIndex.put(value, offset);
                }

            }
        }
    }

    public static class TransactionsCombiner
            extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "job three");
        job.setJarByClass(IndexCreation.class);
        job.setMapperClass(IndexCreation.AgeMapper.class);
        job.setPartitionerClass(CustomPartitioner.class);
        //job.setCombinerClass(JobThree.TransactionsCombiner.class);
        //job.setReducerClass(JobThree.TransactionsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("/home/kevin/cs509/cs509resources/Hadoop++/input"));
        FileOutputFormat.setOutputPath(job, new Path("/home/kevin/cs509/cs509resources/Hadoop++/output"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
