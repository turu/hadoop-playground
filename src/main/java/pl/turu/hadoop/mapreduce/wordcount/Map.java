package pl.turu.hadoop.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Author: Piotr Turek
 */
public class Map extends MapReduceBase
        implements Mapper<LongWritable, Text, Text, IntWritable> {

    private static IntWritable one = new IntWritable(1);

    @Override
    public void map(LongWritable document, Text content,
                    OutputCollector<Text, IntWritable> collector, Reporter reporter)
            throws IOException {
        final String value = content.toString();

        final StringTokenizer tokenizer = new StringTokenizer(value);

        while (tokenizer.hasMoreTokens()) {
            final String token = (String) tokenizer.nextElement();
            final Text word = new Text(token);

            collector.collect(word, one);
        }

    }
}
