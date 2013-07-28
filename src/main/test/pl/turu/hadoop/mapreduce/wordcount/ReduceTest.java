package pl.turu.hadoop.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.fest.assertions.Assertions.assertThat;

/**
 * Author: Piotr Turek
 */
public class ReduceTest {

    private Reduce reduce;
    private final IntWritable one = new IntWritable(1);

    @Before
    public void setUp() throws Exception {
        reduce = new Reduce();
    }

    @After
    public void tearDown() throws Exception {
        reduce = null;
    }

    @Test(expected = NullPointerException.class)
    public void testReduceWithNulls() throws Exception {
        //given
        //when
        reduce.reduce(null, null, null, null);
        //then
    }

    @Test
    public void testReduceWithInput() throws Exception {
        //given
        final Text text = new Text("ala");
        final List<IntWritable> freqs = Collections.nCopies(100, one);
        final StubOutputCollector<Text, IntWritable> collector = new StubOutputCollector<>();

        //when
        reduce.reduce(text, freqs.iterator(), collector, null);

        //then
        assertThat(collector.getIntermediate()).as("Reduce does not sum frequencies properly")
                .containsExactly(
                        new Pair<>(text, new IntWritable(100))
                );

    }
}
