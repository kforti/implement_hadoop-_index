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

public class IndexCreation {
    public static class AgeMapper
            extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            ///TODO: Emit {splitID+attribute, record} - where splitID is the split_start and record contains offset

            System.out.println(key.toString());
            System.out.println(key.toString() + "\n");
//            InputSplit split = context.getInputSplit();
//            System.out.println(String.format("\n\n\n%s", key.toString()));
//            context.write(new Text(fileLocation), new Text("Good"));

            //context.write(new Text(custid), new Text(data));
        }

        public String getSplitID() {
            return "";
        }
    }


    public static class TransactionsReducer
            extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

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
