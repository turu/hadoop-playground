package pl.turu.hadoop.mapreduce.wordcount;

import org.apache.hadoop.mapred.OutputCollector;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

class StubOutputCollector<K, V> implements OutputCollector<K, V> {

    final List<Pair<K, V>> intermediate = new LinkedList<Pair<K, V>>();

    @Override
    public void collect(K k, V v) throws IOException {
        intermediate.add(new Pair<K, V>(k, v));
    }

    List<Pair<K, V>> getIntermediate() {
        return intermediate;
    }

}