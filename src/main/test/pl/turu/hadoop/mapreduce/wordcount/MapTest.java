package pl.turu.hadoop.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Reporter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.fest.assertions.Assertions.assertThat;

/**
 * Author: Piotr Turek
 */
public class MapTest {

    private Map map;
    private final IntWritable one = new IntWritable(1);

    @Before
    public void setUp() throws Exception {
        map = new Map();
    }

    @After
    public void tearDown() throws Exception {
        map = null;
    }

    @Test(expected = NullPointerException.class)
    public void testMapWithNulls() throws Exception {
        //given
        //when
        map.map(null, null, null, null);
        //then
    }

    @Test
    public void testMapWithDocument() throws Exception {
        //given
        final LongWritable document = new LongWritable();
        final Text content = new Text("ala ma kota , a kota ma ala");
        final StubOutputCollector<Text, IntWritable> collector = new StubOutputCollector<Text, IntWritable>();
        final Reporter reporter = null;

        //when
        map.map(document, content, collector, reporter);

        //then
        final List<Pair<Text, IntWritable>> intermediate = collector.getIntermediate();
        assertThat(intermediate).as("Output collection is empty").isNotEmpty();
        assertThat(intermediate).as("Output collection does not contain 8 elements").hasSize(8);
        assertThat(intermediate).as("Wrong sequence").containsExactly(
                new Pair<>(new Text("ala"), one),
                new Pair<>(new Text("ma"), one),
                new Pair<>(new Text("kota"), one),
                new Pair<>(new Text(","), one),
                new Pair<>(new Text("a"), one),
                new Pair<>(new Text("kota"), one),
                new Pair<>(new Text("ma"), one),
                new Pair<>(new Text("ala"), one)
        );
    }
}
