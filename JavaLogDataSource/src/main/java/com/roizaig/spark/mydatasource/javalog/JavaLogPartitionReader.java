package com.roizaig.spark.mydatasource.javalog;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.unsafe.types.UTF8String;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Created by Roi Zaig on 2021-03-18.
 */
public class JavaLogPartitionReader implements PartitionReader<InternalRow> {

    private final String filename;
    private final JavaLogReader reader;
    private final String regexp;

    public JavaLogPartitionReader(String filename, String regexp) throws FileNotFoundException {
        this.filename = filename;
        this.regexp = regexp;
        this.reader = new JavaLogReader(filename);
    }

    @Override
    public boolean next() {
        return this.reader.hasNext();
    }

    @Override
    public InternalRow get() {
        List<Object> values = new ArrayList<>();
        String line = this.reader.next();
        Pattern p = Pattern.compile(this.regexp);
        Matcher m = p.matcher(line);
        if (m.matches() && m.groupCount() >= 6) {
            String dateTime = m.group(1);
            String thread = m.group(2);
            String logLevel = m.group(3);
            String className = m.group(4);
            String lineNumber = m.group(5);
            String message = m.group(6);
            values.add(getSafeUTF8String(dateTime));
            values.add(getSafeUTF8String(thread));
            values.add(getSafeUTF8String(logLevel));
            values.add(getSafeUTF8String(className));
            values.add(getSafeUTF8String(lineNumber));
            values.add(getSafeUTF8String(message));
        } else {
            throw new JavaLogUtils.MatcheException(line, p.pattern(), filename);
        }
        return new GenericInternalRow(values.toArray());
    }

    private UTF8String getSafeUTF8String(String s) {
        return UTF8String.fromString(s == null ? "" : s.trim());
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}
