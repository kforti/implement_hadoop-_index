package index;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class CustomPartitioner extends Partitioner<Text,Text> {

    public int getPartition(Text key, Text value, int numReduceTasks){
        // TODO: partition mapped keys bases on splitID

        String splitID = key.toString().split(",")[0];
        return (splitID.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
}
