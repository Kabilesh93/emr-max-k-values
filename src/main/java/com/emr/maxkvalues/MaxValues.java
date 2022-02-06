package com.emr.maxkvalues;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.List;
import java.util.ListIterator;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MaxValues {

    public static class Map extends Mapper<Object, Text, Text, Text> {
        private TreeMap<Long, Integer> topNValuesMap;

        @Override
        public void setup(Context context) {
            topNValuesMap = new TreeMap<>();
        }

        @Override
        public void map(Object key, Text value, Context context) {
            Configuration conf = context.getConfiguration();
            int count = Integer.parseInt(conf.get("count"));

            List<Integer> items = Stream.of(value.toString().split(" "))
                    .map(String::trim).map(Integer::parseInt).collect(Collectors.toList());
            ListIterator<Integer> iterator = items.listIterator();

            while (iterator.hasNext()) {
                int index = iterator.nextIndex();
                topNValuesMap.put(Long.valueOf(iterator.next()), index);
            }

            if (topNValuesMap.size() > count) {
                topNValuesMap.remove(topNValuesMap.firstKey());
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            for (java.util.Map.Entry<Long, Integer> entry : topNValuesMap.entrySet()) {
                long number = entry.getKey();
                int index = entry.getValue();
                context.write(new Text(String.valueOf(number)), new Text(String.valueOf(index)));
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        private TreeMap<Long, Integer> topNValuesReducerMap = new TreeMap<>();

        @Override
        public void setup(Context context){
            topNValuesReducerMap = new TreeMap<>();
        }

        public void reduce(Text key, Iterable<Text> values, Context context) {

            Configuration conf = context.getConfiguration();
            int count = Integer.parseInt(conf.get("count"));

            topNValuesReducerMap.put(Long.valueOf(key.toString()), Integer.valueOf(key.toString()));
            if (topNValuesReducerMap.size() > count) {
                topNValuesReducerMap.remove(topNValuesReducerMap.firstKey());
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            int rank = 1;
            for (Long entry : topNValuesReducerMap.descendingKeySet()) {
                context.write(new Text(String.valueOf(rank)), new Text(String.valueOf(entry)));
                rank++;
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.set("count", "10");

        Job job = Job.getInstance(conf, "maxkvalues");

        job.setJarByClass(MaxValues.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
