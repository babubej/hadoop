package com.hortonworks.target.dse.hive;

import org.apache.hadoop.hive.ql.exec.UDF;
import java.util.Arrays;
import java.util.List;

public class deepEqualsUDF extends UDF{
    public boolean evaluate(List<String> first, List<String> second ) {
        return Arrays.deepEquals(first.toArray(), second.toArray());
    }
}
