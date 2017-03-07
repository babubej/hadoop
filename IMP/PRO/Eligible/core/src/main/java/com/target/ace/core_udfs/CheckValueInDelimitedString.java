package com.target.ace.core_udfs;

import java.io.IOException;
import java.io.FileReader;
import java.io.BufferedReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class CheckValueInDelimitedString extends EvalFunc<Boolean> {

    String delimiter;
    
    public CheckValueInDelimitedString(String delimiter) {
        this.delimiter = delimiter;
    }

    public Boolean exec(Tuple input) throws IOException {
        if (input == null || input.size() < 2) {
           System.err.println("Error: CheckValueInDelimitedString requires two parameters delimited-string and string to check. No values are specifiedi, skipping lookup.");
           return null;
        }

        if (input.get(0) == null) return null;
        
        List<String> values = Arrays.asList((input.get(0).toString()).split(delimiter));
        
        return values.contains((String)input.get(1));
    }

}
