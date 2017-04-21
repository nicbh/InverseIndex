/**
 * Created by hadoop on 21/04/17.
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class InvertedIndex {
    public static class InvertedIndexMapper extends Mapper<Object,Text,Text,IntWritable>{

        private IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private FileSplit split;
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            split = (FileSplit)context.getInputSplit();
            while (itr.hasMoreTokens()){
                word.set(itr.nextToken()+":"+split.getPath());
                context.write(word,one);
            }
        }
    }
    public static class InvertedIndexCombiner extends Reducer<Text,IntWritable,Text,Text> {

        private Text result = new Text();

        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable val:values) {
                sum += val.get();
            }
            int splitIndex = key.toString().indexOf(":");
            result.set(key.toString().substring(splitIndex+1)+":"+sum+";");
            key.set(key.toString().substring(0,splitIndex));
            context.write(key,result);
        }
    }

    public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text>{
        private Text value = new Text();
        protected void reduce(Text key, Iterable<Text>values, Context context) throws IOException, InterruptedException{
            String result = new String();
            for(Text info: values)
                result+=info.toString();
            value.set(result);
            context.write(key,value);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // write your code here

        Configuration configuration = new Configuration();
        if(args.length!=2){
            System.err.println("Usage:wordcount <input><output>");
            System.exit(2);
        }

        Job job = new Job(configuration,"word count");

        job.setJarByClass(InvertedIndex.class);
        job.setMapperClass(InvertedIndexMapper.class);
        job.setCombinerClass(InvertedIndexCombiner.class);
        job.setReducerClass(InvertedIndexReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        System.exit(job.waitForCompletion(true)?0:1);
    }
}
