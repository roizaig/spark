package com.roizaig.spark.mydatasource.javalog;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.Iterator;

/**
 * Created by Roi Zaig on 2021-03-18.
 */
public class JavaLogReader implements Iterator<String> {

    private Reader reader;

    public JavaLogReader(String filename) throws FileNotFoundException {
        this.reader = new FileReader(filename);
    }

    @Override
    public boolean hasNext() {
        try {
            return this.reader.ready();
        } catch (IOException e) {
            return false;
        }
    }

    /**
     * @return a FULL log message. May be multiline.
     */
    @Override
    public String next() {
        StringBuilder sb = new StringBuilder();

        // read line

        try {
            int c;
            while ((c = this.reader.read()) != -1 && c != '\n') {
                sb.append((char) c);
            }

            if (c == -1) {
                this.reader.close();
            }
        } catch (IOException e) {
            return null;
        }


        return sb.toString().trim();
    }

    public void close() throws IOException {
        reader.close();
    }
}
