import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class Chi_Value_Job {




    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text> {


        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String[] split = value.toString().split("\\s+");

            context.write(new Text(split[0]), new Text(split[1] + " " + split[2] + " " + split[3] + " " + split[4] + " " + split[5]));

        }
    }

    public static class Combiner
            extends Reducer<Text, Text, TextPair, DoubleWritable> {

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {

            int sum = 0;

            Text A = new Text();
            Text Nt = new Text();
            Text Nc = new Text();
            Text N = new Text();

            Text Terms = new Text();

            double Chi = 0;

            double B = 0;
            double C = 0;
            double D = 0;

            for (Text text : values) {
                Terms = new Text(text.toString().split("\\s+")[0]);

                A = new Text(text.toString().split("\\s+")[1]);
                Nt = new Text(text.toString().split("\\s+")[2]);
                Nc = new Text(text.toString().split("\\s+")[3]);
                N = new Text(text.toString().split("\\s+")[4]);

                B = Double.parseDouble(Nt.toString()) - Double.parseDouble(A.toString());
                C = Double.parseDouble(Nc.toString()) - Double.parseDouble(A.toString());
                D = Double.parseDouble(N.toString()) - Double.parseDouble(Nt.toString()) - (Double.parseDouble(Nc.toString()) - Double.parseDouble(A.toString()));

                Chi = Double.parseDouble(N.toString()) * Math.pow((Integer.parseInt(A.toString()) * D - B * C), 2) / ((Double.parseDouble(A.toString()) + C) * (B + D) * (Double.parseDouble(A.toString()) + B) * (C + D));

                context.write(new TextPair(key, Terms), new DoubleWritable(Chi));
            }


        }
    }
}
