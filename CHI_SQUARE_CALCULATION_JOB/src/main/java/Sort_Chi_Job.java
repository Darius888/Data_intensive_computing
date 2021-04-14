import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.*;


import java.io.IOException;
import java.util.ArrayList;


public class Sort_Chi_Job {


    public static class NaturalKeyGroupingComparator extends WritableComparator {

        protected NaturalKeyGroupingComparator() {
            super(TextPairChi.class, true);
        }

        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            TextPairChi k1 = (TextPairChi) w1;
            TextPairChi k2 = (TextPairChi) w2;

            return k1.getSecond().compareTo(k2.getSecond());

        }
    }

    public static class NaturalKeyPartitioner extends Partitioner<TextPairChi, Text> {

        @Override
        public int getPartition(TextPairChi key, Text val, int numPartitions) {
            return (key.getSecond().hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }


    public static class TokenizerMapper
            extends Mapper<Object, Text, TextPairChi, Text> {


        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String[] split = value.toString().split("\\s+");

            context.write(new TextPairChi(new Text(split[0]), new DoubleWritable(Double.parseDouble(split[2]))), new Text(split[1]));

        }
    }

    public static class IntSumReducer
              extends Reducer<TextPairChi, Text, Text, Text> {
        long counter = 0;
        Text prev = null;
        public void reduce(TextPairChi key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            for(Text text: values)
            {
                if(prev!=null)
                {
                    if(!key.getFirst().toString().equals(prev.toString()))
                    {
                        counter=0;
                    }
                }

                context.write(new Text(key.getFirst().toString()), new Text(text.toString() + " " + key.getSecond().toString() + " " + counter));
                counter++;
                prev = new Text(key.getFirst().toString());
            }
    }
}


//    public static class IntSumReducer
//            extends Reducer<TextPairChi, Text, Text, Text> {
//
//        private MultipleOutputs<Text,Text> out;
//        @Override
//        public void setup(Context context) {
//            out = new MultipleOutputs<Text,Text>(context);
//        }
//
//        public void reduce(TextPairChi key, Iterable<Text> values,
//                           Context context
//        ) throws IOException, InterruptedException {
//            ArrayList<Text> valuesz = new ArrayList<>();
//            for(Text text: values)
//            {
//                valuesz.add(new Text(key.getSecond() + " " + text.toString()));
//            }
////            out.write("tmpoutput",new Text("A"), new Text("A"), "tmpoutput/"+"A");
//
//            for(Text text: values) {
//                out.write(new Text("A"), new Text("A"), "tmpoutput1/agagag");
//            }
//        }
//        @Override
//        public void cleanup(Context context) throws IOException,InterruptedException {
////            out.write("tmpoutput",new Text("A"), new Text("A"), "tmpoutput/"+"A");
//
////            out.write(new Text("A"), new Text("A"),"tmpoutput/A.txt");
//
//            out.close();
//        }
//        }


//    public static class IntSumReducer
//            extends Reducer<TextPairChi, Text, Text, Text> {
//
//        private TreeMap<Double, Text> tmap2;
//
//        @Override
//        public void setup(Context context) throws IOException,
//                InterruptedException
//        {
//            tmap2 = new TreeMap<Double, Text>();
//        }
//
//        public void reduce(TextPairChi key, Iterable<Text> values,
//                           Context context
//        ) throws IOException, InterruptedException {
//
//         for (Text text : values) {
//             tmap2.put(Double.parseDouble(key.getSecond().toString()), new Text(key.getFirst().toString() + " " + text.toString() + " "));
//         }
//            if (tmap2.size() > 150)
//            {
//                tmap2.remove(tmap2.firstKey());
//            }
//
//        }
//
//        @Override
//        public void cleanup(Context context) throws IOException,
//                InterruptedException
//        {
//
//            for (Map.Entry<Double, Text> entry : tmap2.entrySet())
//            {
//
//                Double count = entry.getKey();
//                Text name = entry.getValue();
//                context.write(new Text(name), new Text(count.toString()));
//            }
//        }
//    }
//}
//     public static class IntSumReducer
//            extends Reducer<TextPairChi, Text, Text, Text> {
//        long counteris = 0;
//        Text prev = null;
//        StringBuilder stringBuilder = new StringBuilder();
//        ArrayList<Text> arrayList = new ArrayList<>();
//
//    public void reduce(TextPairChi key, Iterable<Text> values,
//                           Context context
//        ) throws IOException, InterruptedException {
//
//
//
//            for (Text text : values) {
//                arrayList.add(new Text(key.getFirst().toString() + " " + text.toString() + " " + key.getSecond().toString() + " "));
//            }
//
//            Text prev2 = null;
//            for(Text text:arrayList)
//            {
////                System.out.println("AAAAAAAAA" + text.toString().split("\\s+")[1] + "  " + text.toString().split("\\s+")[2] + " ");
//                if (counteris < 150) {
////                    System.out.println(text.toString().split("\\s+")[1] + ":" + text.toString().split("\\s+")[2] + " ");
//                    stringBuilder.append(text.toString().split("\\s+")[1] + ":" + text.toString().split("\\s+")[2] + " ");
//                }
//
//                if (counteris != 0) {
//
//                    if (!(text.toString().split("\\s+")[0].equals(prev.toString()))) {
//
////                        System.out.println(prev.toString() + " " + stringBuilder.toString());
//                        context.write(new Text(prev.toString()), new Text(stringBuilder.toString()));
//                        stringBuilder = new StringBuilder();
//                        counteris = 0;
//                    }
//
//                }
//
//
//                prev = new Text(text.toString().split("\\s+")[0]);
//
//                counteris++;
//            }
////            context.write(new Text(prev.toString()), new Text(stringBuilder.toString()));
//
//        }
//    }



//    public static class IntSumReducer
//            extends Reducer<TextPairChi, Text, Text, Text> {
//        long counteris = 0;
//        Text prev = null;
//        StringBuilder stringBuilder = new StringBuilder();
//
//        Double aDouble = new Double(4.6666);
//        Double aDoubles =new Double(5.3333);
//
//        ArrayList<Text> elems= new ArrayList<>();
//        ArrayList<Text> elems2= new ArrayList<>();
//        public void reduce(TextPairChi key, Iterable<Text> values,
//                           Context context
//        ) throws IOException, InterruptedException {
//
//            for (Text text : values) {
//                if (counteris < 150) {
//                    elems.add(new Text(key.getFirst().toString() + " " + text.toString() + " " + key.getSecond().toString()));
//                    elems2.add(new Text(key.getFirst().toString() + " " + text.toString() + " " + key.getSecond().toString()));
//                }
//
//                if(counteris > 150 && (key.getFirst().toString().equals(prev.toString())))
//                {
//
//                    for (Text elem : elems) {
//                        if (elem.toString().split("\\s+")[0].equals(key.getFirst().toString()) && Double.compare(Double.parseDouble(elem.toString().split("\\s+")[2]), Double.parseDouble(key.getSecond().toString())) < 0) {
//                            elem.set(new Text(key.getFirst().toString() + " " + text.toString() + " " + key.getSecond().toString()));
//                        }
//                        if (elem.toString().split("\\s+")[0].equals(key.getFirst().toString()) && Double.parseDouble(elem.toString().split("\\s+")[2]) < Double.parseDouble(key.getSecond().toString())) {
//                            elem.set(new Text(key.getFirst().toString() + " " + text.toString() + " " + key.getSecond().toString()));
//                        }
//                    }
////
//                }
//
//                if (counteris != 0) {
//                    if (!(key.getFirst().toString().equals(prev.toString()))) {
//                        for(Text text1:elems)
//                        {
//                            stringBuilder.append(text1.toString().split("\\s+")[1] + ":" + text1.toString().split("\\s+")[2] + " ");
//                        }
//                        context.write(new Text(prev.toString()), new Text(stringBuilder.toString()));
//                        stringBuilder = new StringBuilder();
//                        counteris = 0;
//                        elems = new ArrayList<>();
//                    }
//                }
//                prev = new Text(key.getFirst().toString());
//                counteris++;
//            }
//
//
//
//        }
////        @Override
////        public void cleanup(Context context) throws IOException, InterruptedException
////        {
////            context.write(new Text(prev.toString()), new Text(stringBuilder.toString()));
////        }
//    }

}
