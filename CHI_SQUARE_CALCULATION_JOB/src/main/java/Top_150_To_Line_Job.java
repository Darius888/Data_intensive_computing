import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Top_150_To_Line_Job {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text> {
        long counter = 0;
        long counter2 = 0;
        String prev = "";
        StringBuilder stringBuilder = new StringBuilder();
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String[] split = value.toString().split("\\s+");



            if(!split[1].equals(prev) && !prev.isEmpty())
            {
                context.write(new Text(prev), new Text(stringBuilder.toString()));
                stringBuilder = new StringBuilder();
                counter=0;
                prev = split[1];
            }

            if(((Integer.parseInt(split[4]) < 150 && split[1].equals(prev)) || (prev.isEmpty())))
            {

                stringBuilder.append(split[2] + ":" + split[3] + " ");

                counter++;
            } else if(!split[1].equals(prev))
            {
                counter = 0;
            }



            if(counter2 == 0)
            {
                prev = split[1];
            }
            counter2++;

        }
    }

}
