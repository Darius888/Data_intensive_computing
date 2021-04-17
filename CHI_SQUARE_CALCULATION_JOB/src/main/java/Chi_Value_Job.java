import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/**
 * 10th job responsible for calculating chi square values for each term.
 */
public class Chi_Value_Job {

    public static class Mapper1
            extends Mapper<Object, Text, Text, Text> {

        /**
         * @param key : is the line offset
         * @param value : the actual line with text
         * @param context : object through which output is emitted as well as progress is reported
         * In this map method each line is received as "category term A Nt Nc N"
         * Then this line is splitted through whitespaces.
         * Then such key value pairs are emitted < key: category, value: term, Nt, Nc, N >
         */
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String[] split = value.toString().split("\\s+");

            context.write(new Text(split[0]), new Text(split[1] + " " + split[2] + " " + split[3] + " " + split[4] + " " + split[5]));
        }
    }

    public static class Reducer1
            extends Reducer<Text, Text, TextPair, DoubleWritable> {

        /**
         * @param key : key value received from mapper
         * @param values : values which belong to each key. They are received as Iterable.
         * @param context : object through which output is emitted as well as progress is reported
         * In this reduce method such key value pairs are received from mapper < key: category, value: term, Nt, Nc, N >
         * Then all values are converted to doubles
         * Then for each term chi^2 value is calculated
         * Then we output such key values pairs < key:category term, value:chi^2 value >
         */
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {

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

                //DOUBLE HERE ?
                Chi = Double.parseDouble(N.toString()) * Math.pow(((Double.parseDouble(A.toString()) * D) - (B * C)), 2) / ((Double.parseDouble(A.toString()) + C) * (B + D) * (Double.parseDouble(A.toString()) + B) * (C + D));

                context.write(new TextPair(key, Terms), new DoubleWritable(Chi));
            }
        }
    }
}
