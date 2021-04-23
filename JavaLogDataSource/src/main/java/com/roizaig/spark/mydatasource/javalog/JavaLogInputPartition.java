package com.roizaig.spark.mydatasource.javalog;

import org.apache.spark.sql.connector.read.InputPartition;

/**
 *  Created by Roi Zaig on 2021-03-17. 
 */
public class JavaLogInputPartition implements InputPartition {

    @Override
    public String[] preferredLocations() {
        return new String[0];
    }
}
