package com.roizaig.spark.mydatasource.javalog;

import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 *  Created by Roi Zaig on 2021-03-17. 
 */
public class JavaLogScan implements Scan {

    private final CaseInsensitiveStringMap options;

    public JavaLogScan(CaseInsensitiveStringMap options) {
        this.options = options;
    }

    @Override
    public StructType readSchema() {
        return JavaLogUtils.javaLogSchema();
    }

    @Override
    public Batch toBatch() {
        return new JavaLogBatch(options);
    }

    @Override
    public String description() {
        return "Java Log ";
    }
}
