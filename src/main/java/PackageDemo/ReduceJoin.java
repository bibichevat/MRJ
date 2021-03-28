package PackageDemo;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ReduceJoin {
    public static class MapForWordsFirstFile extends Mapper <LongWritable, Text, Text, IntWritable>
    {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException
        {
            String line = value.toString();
            String[] words=line.split(" ");
            for(String word: words )
            {
                Text outputKey = new Text(word.toUpperCase().trim());
                IntWritable outputValue = new IntWritable(1);
                con.write(outputKey, outputValue);
            }
        }
    }
    public static class MapForWordsSecondFile extends Mapper <LongWritable, Text, Text, IntWritable>
    {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException
        {
            String line = value.toString();
            String[] words=line.split(" ");
            for(String word: words )
            {
                Text outputKey = new Text(word.toUpperCase().trim());
                IntWritable outputValue = new IntWritable(2);
                con.write(outputKey, outputValue);
            }
        }
    }

    public static class ReduceJoinReducer extends Reducer <Text, IntWritable, Text, IntWritable>
    {
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException
        {
            boolean f = true;
            for (IntWritable value : values)
            {
                if (value.get() == 2)
                {
                    f = false;
                }
            }
            if (f) {
                context.write(key, new IntWritable(1));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "Reduce-side join");
        job.setJarByClass(ReduceJoin.class);
        job.setReducerClass(ReduceJoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class, MapForWordsFirstFile.class);
        MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, MapForWordsSecondFile.class);
        Path outputPath = new Path(args[2]);

        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}