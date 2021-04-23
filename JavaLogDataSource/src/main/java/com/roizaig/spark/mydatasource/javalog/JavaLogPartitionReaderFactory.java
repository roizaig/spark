package com.roizaig.spark.mydatasource.javalog;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;

import java.io.FileNotFoundException;

/**
 *  Created by Roi Zaig on 2021-03-17. 
 */
public class JavaLogPartitionReaderFactory implements PartitionReaderFactory {

    private final String filename;
    private final String regexp;

    public JavaLogPartitionReaderFactory(String filename, String regexp) {
        this.filename = filename;
        this.regexp = regexp;
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
        try {
            return new JavaLogPartitionReader(filename, regexp);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            throw new RuntimeException();
        }
    }

}
