package bigdata.ass1.q3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;


public class SortJobDriver {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: SortJobDriver <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Sort Key-Value Pairs");
        job.setJarByClass(SortJobDriver.class);

        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

//        job.setPartitionerClass(TotalOrderPartitioner.class);
//        job.setNumReduceTasks(4);
//
//        // sample and write partition file
//        InputSampler.Sampler<Text, Text> sampler =
//                new InputSampler.RandomSampler<Text, Text>(0.1, 10000, 10);
//        InputSampler.writePartitionFile(job, sampler);
//
//        // set partition file path
//        TotalOrderPartitioner.setPartitionFile(job.getConfiguration(), new Path("_partitions.lst"));

        // use one reducer to guarantee that data will be globally sorted
        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
