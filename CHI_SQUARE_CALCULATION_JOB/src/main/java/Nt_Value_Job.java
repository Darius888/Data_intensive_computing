import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Nt_Value_Job {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String[] splited = value.toString().split("\\s+");

            context.write(new Text(splited[1]), new IntWritable(Integer.parseInt(splited[2])));

        }
    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;

            for (IntWritable model : values) {
                sum += model.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }


}
