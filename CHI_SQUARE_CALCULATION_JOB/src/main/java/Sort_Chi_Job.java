import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;


import java.io.IOException;
import java.util.ArrayList;


public class Sort_Chi_Job {

    public static class NaturalKeyGroupingComparator extends WritableComparator {
        protected NaturalKeyGroupingComparator() {
            super(TextPairChi.class, true);
        }
        @SuppressWarnings("rawtypes")
        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            TextPairChi k1 = (TextPairChi)w1;
            TextPairChi k2 = (TextPairChi)w2;

            return k1.getSecond().compareTo(k2.getSecond());
        }
    }

    public static class NaturalKeyPartitioner extends Partitioner<TextPairChi, Text> {

        @Override
        public int getPartition(TextPairChi key, Text val, int numPartitions) {
            return key.getSecond().hashCode() % numPartitions;
        }

    }


    public static class TokenizerMapper
            extends Mapper<Object, Text, TextPairChi, Text> {


        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String[] split = value.toString().split("\\s+");

            context.write(new TextPairChi(new Text(split[0]), new DoubleWritable(Double.parseDouble(split[2]))),new Text(split[1]));

        }
    }



    public static class IntSumReducer
            extends Reducer<TextPairChi, Text, Text, Text> {

        public void reduce(TextPairChi key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {

            ArrayList<Text> tmp1 = new ArrayList<>();
            ArrayList<Text> tmp2 = new ArrayList<>();

            for(Text text: values)
            {

                    context.write(new Text(key.toString().split("\\s+")[0]),new Text(text + " " + key.toString().split("\\s+")[1]));


            }
        }
    }
}
