package com.roizaig.spark.mydatasource.javalog;

import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 *  Created by Roi Zaig on 2021-03-17. 
 */
public class JavaLogScanBuilder implements ScanBuilder {

    private final CaseInsensitiveStringMap options;

    public JavaLogScanBuilder(CaseInsensitiveStringMap options) {
        this.options = options;
    }

    @Override
    public Scan build() {
        return new JavaLogScan(options);
    }

}
