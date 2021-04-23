package com.roizaig.spark.mydatasource.javalog;

import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Roi Zaig on 2021-03-17.
 */
public class DefaultSource implements TableProvider {

    @Override
    public StructType inferSchema(CaseInsensitiveStringMap options) {
        return getTable(null, new Transform[] {}, new HashMap<>()).schema();
    }

    @Override
    public Table getTable(StructType schema, Transform[] partitioning, Map<String, String> properties) {
        return new JavaLogBatchTable();
    }
}
