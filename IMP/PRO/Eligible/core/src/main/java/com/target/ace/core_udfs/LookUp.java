package com.target.ace.core_udfs;

import java.io.IOException;
import java.io.FileReader;
import java.io.BufferedReader;
import java.util.ArrayList;
import java.util.List;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class LookUp extends EvalFunc<Boolean> {

    String filePath;
    String fileName;
    String lookupFile;
    ArrayList<String> lookup = null;
    
    public LookUp(String path, String fileName) {
        this.filePath = path;
        this.fileName = fileName;
        this.lookupFile = path+fileName;
    }

    public Boolean exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0) {
           throw new IOException("Error: No Lookup Information (key,value) Specified");
        }  
        if (lookup == null) {
            lookup = new ArrayList<String>();
            FileReader fileRead = new FileReader("./"+fileName);
            BufferedReader bufRead = new BufferedReader(fileRead);
            String line;
            while ((line = bufRead.readLine()) != null) {
                lookup.add(line);
            }
            fileRead.close();
        }
        
        return lookup.contains((String)input.get(0));
    }

    public List<String> getCacheFiles() {
        List<String> list = new ArrayList<String>(1);
        list.add(lookupFile +"#"+fileName);
        return list;
    }
}
