package com.roizaig.spark.mydatasource.javalog;

import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 *  Created by Roi Zaig on 2021-03-17. 
 */
public class JavaLogBatch implements Batch {

    private final CaseInsensitiveStringMap options;

    public JavaLogBatch(CaseInsensitiveStringMap options) {
        this.options = options;
    }

    @Override
    public InputPartition[] planInputPartitions() {
        // single partition
        return new InputPartition[]{ new JavaLogInputPartition() };
    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        String filename = options.get(JavaLogUtils.JAVALOG_OPTION_FILENAME);
        String regexp = options.get(JavaLogUtils.JAVALOG_OPTION_REGEXP);
        return new JavaLogPartitionReaderFactory(filename, regexp);
    }

}
