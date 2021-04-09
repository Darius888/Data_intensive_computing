import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class A_Value_Job {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable ONE = new IntWritable(1);
        private Text term = new Text();


        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String[] splited = value.toString().split("\\s+");

            //0 - category, 1 - term
            Text val = new Text(splited[0]  + " " + splited[1]);
            context.write(val, ONE);

        }
    }

    public static class Combinatorika
            extends Reducer<A_Value_Model, IntWritable, A_Value_Model, IntWritable> {

        public void reduce(A_Value_Model key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {

            A_Value_Model newKey = new A_Value_Model(new Text(key.getSecond()),new Text(key.getFirst()));
            context.write(newKey,new IntWritable(1));

        }
    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;

            for (IntWritable model: values)
            {
                sum+=model.get();

            }
            context.write(key,new IntWritable(sum));
        }
    }

}
