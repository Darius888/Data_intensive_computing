import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.*;


public class Top150_Job {

    public static class TokenizerMapper
            extends Mapper<Object, Text, IntWritable, Text> {

        long counteris = 0;
        String prev = "";
        ArrayList<Text> arrayList = new ArrayList<>();
        StringBuilder stringBuilder = new StringBuilder();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String[] split = value.toString().split("\\s+");

            if(Integer.parseInt(split[3]) < 150)
            {
                arrayList.add(new Text(value));
                context.write(new IntWritable((int) counteris), new Text(value));
            }

            prev = split[0];
        }
    }

    public static class IntSumReducer
            extends Reducer<IntWritable, Text, Text, Text> {

        String prev = "";
        StringBuilder stringBuilder = new StringBuilder();
        Text textas = new Text();
        ArrayList<Text> arrayList = new ArrayList<>();
        HashSet<Text> hashSet = new HashSet<>();
        public void reduce(IntWritable key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {

            for(Text text:values)
            {

                if(Integer.parseInt(text.toString().split("\\s+")[3]) <= 150)
                {
                    arrayList.add(new Text(text));
                }
            }

            Collections.reverse(arrayList);

            for(int i =0; i < arrayList.size(); i++)
            {

                if((!arrayList.get(i).toString().split("\\s+")[0].equals(prev) && !prev.isEmpty()) || arrayList.size()-1 == i)
                {
                    context.write(new Text(prev), new Text(stringBuilder.toString()));
                    stringBuilder = new StringBuilder();
                }

                    if(Integer.parseInt(arrayList.get(i).toString().split("\\s+")[3]) < 150)
                    {
                        if(arrayList.get(i).toString().split("\\s+")[1].equals("big"))
                        {
                            System.out.println(arrayList.get(i).toString());
                        }
                        stringBuilder.append(arrayList.get(i).toString().split("\\s+")[1] + ":" + arrayList.get(i).toString().split("\\s+")[2] + " ");
                    }

                prev = arrayList.get(i).toString().split("\\s+")[0];
            }

        }
    }
}
