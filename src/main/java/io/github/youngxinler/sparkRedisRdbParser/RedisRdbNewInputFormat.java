package io.github.youngxinler.sparkRedisRdbParser;

import net.whitbeck.rdbparser.Entry;
import net.whitbeck.rdbparser.EntryType;
import net.whitbeck.rdbparser.KeyValuePair;
import net.whitbeck.rdbparser.RdbParser;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.io.InputStream;

public class RedisRdbNewInputFormat extends FileInputFormat<String, KeyValuePair> {
    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }

    @Override
    public RecordReader createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new RedisRdbRecordReaderNewApi();
    }

    class RedisRdbRecordReaderNewApi extends RecordReader<String, KeyValuePair> {
        private RdbParser rdbParser;
        private long allBytes;
        private String currentKey;
        private KeyValuePair currentValue;


        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) split;
            FileSystem fs = FileSystem.get(context.getConfiguration());
            InputStream inputStream = fs.open(fileSplit.getPath());
            this.allBytes = fileSplit.getLength();
            this.rdbParser = new RdbParser(inputStream);
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            Entry entry = null;
            while ((entry = rdbParser.readNext()) != null) {
                if (entry.getType() == EntryType.KEY_VALUE_PAIR) {
                    KeyValuePair keyValuePair = (KeyValuePair) entry;
                    this.currentKey = new String(keyValuePair.getKey());
                    this.currentValue = keyValuePair;
                    return true;
                }
            }
            this.currentKey = null;
            this.currentValue = null;
            return false;
        }

        @Override
        public String getCurrentKey() throws IOException, InterruptedException {
            return this.currentKey;
        }

        @Override
        public KeyValuePair getCurrentValue() throws IOException, InterruptedException {
            return this.currentValue;
        }


        @Override
        public float getProgress() throws IOException, InterruptedException {
            return (float)rdbParser.bytesParsed() / allBytes;
        }

        @Override
        public void close() throws IOException {
            rdbParser.close();
        }
    }
}
