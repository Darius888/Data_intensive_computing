
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.Text;


import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class Nt_Value_Job {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable ONE = new IntWritable(1);
        private Text term = new Text();


        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String[] splited = value.toString().split("\\s+");


            //0 - category, 1 - term, 2 - A_value
            Text val = new Text(splited[1]);

            context.write(new Text(splited[1]), new IntWritable(Integer.parseInt(splited[2])));

        }
    }

    public static class Combiner
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {

            context.write(new Text(key.toString().split(":")[1]),new IntWritable(1));


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
                System.out.println("AAA" + model);
                sum+=model.get();
            }

            context.write( key,new IntWritable(sum));


        }
    }


}
