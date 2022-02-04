package com.emr.maxkvalues;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MaxValues {

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable index = new IntWritable(1);
        private final Text numberValue = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int count = Integer.parseInt(conf.get("count"));

            List<Integer> items = Stream.of(value.toString().split(" "))
                    .map(String::trim).map(Integer::parseInt)
                    .sorted(Comparator.reverseOrder()).limit(count).collect(Collectors.toList());
            ListIterator<Integer> iterator = items.listIterator();

            while (iterator.hasNext()) {
                index.set(iterator.nextIndex());
                numberValue.set(String.valueOf(iterator.next()));
                context.write(numberValue, index);
            }
        }
    }

    public static class Reduce extends Reducer<NullWritable, IntWritable, NullWritable, IntWritable> {
        private final TreeMap<Integer, Integer> topNValuesMap = new TreeMap<>();

        public void reduce(NullWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            Configuration conf = context.getConfiguration();
            int count = Integer.parseInt(conf.get("count"));

            topNValuesMap.put(Integer.valueOf(key.toString()), Integer.valueOf(key.toString()));
            if (topNValuesMap.size() > count) {
                topNValuesMap.remove(topNValuesMap.firstKey());
            }
            for (Integer i : topNValuesMap.descendingMap().values()) {
                context.write(NullWritable.get(), new IntWritable(i));
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.set("count", "5");

        Job job = Job.getInstance(conf, "maxkvalues");

        job.setJarByClass(MaxValues.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
