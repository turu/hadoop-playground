package pl.turu.hadoop.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

/**
 * Author: Piotr Turek
 */
public class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(Text text, Iterator<IntWritable> intWritableIterator,
                       OutputCollector<Text, IntWritable> collector, Reporter reporter) throws IOException {
        int result = 0;
        while (intWritableIterator.hasNext()) {
            final IntWritable next = intWritableIterator.next();
            result += next.get();
        }

        final IntWritable outputWritable = new IntWritable(result);
        collector.collect(text, outputWritable);
    }

}
