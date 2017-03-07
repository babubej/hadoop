package pigunit;

import org.apache.pig.pigunit.PigTest;
import org.apache.pig.tools.parameters.ParseException;
import org.junit.Test;
import java.io.File;
import java.io.FileWriter;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.pig.pigunit.Cluster;
import org.apache.pig.pigunit.PigTest;
import org.apache.pig.pigunit.pig.PigServer;
import org.apache.pig.test.Util;
import org.apache.pig.tools.parameters.ParseException;
import org.apache.pig.impl.util.PropertiesUtil;
import org.junit.BeforeClass;
import java.io.IOException;
import org.apache.pig.ExecType;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.util.PropertiesUtil;
import java.util.*;
import java.io.*;
import java.nio.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import org.apache.pig.data.Tuple;
import junit.framework.Assert;
import org.junit.*;

public class CoreRplEligibilityTest {
    private static PigServer pigServer ;
    private static PigContext pigContext ;
    private static Cluster cluster ;
    private static PigTest test ;
    private static final Log LOG = LogFactory.getLog(CoreItemLocationCombinedFactTest.class);
    private String params[];
    private String pigScript;
    private String aliasToCompare;
    private String expectedFile;
    
    @BeforeClass
    public static void setUpOnce() throws Exception {
        pigServer = new PigServer(ExecType.LOCAL);
        pigContext = pigServer.getPigContext();
        cluster = new Cluster(pigContext);
    }

    @Before 
    public void setUp() throws IOException {
       params = new String [] {
            "JOB_NAME=CORE-RPL-ELIGIBILITY",
            "QUEUE_NAME=etl",
            "MAX_SPLIT_SIZE=268435456",
            "DATE_STAMP=2015-07-20",
            "INPUT_PATH=src/test/resources/pigunit/input/core/item_loc_combined_fact/dt=2015-07-20/*",
            "INPUT_SBT_PATH=src/test/resources/pigunit/input/extractor/sbt/dt=2015-07-20/*",
            "OUTPUT_PATH=build/output/eligibility_rpl/",
            "LOOKUP_PATH=src/main/resources/lookup_data/",
            "RPL_ITEM_STATUS_CODE=rpl_item_status_code",
            "ACE_CORE_UDFS=build/libs/ace-core-1.1.jar" 
        };
        pigScript = "./src/main/pig/rpl_eligibility.pig";
        aliasToCompare = "rpl_eligibility";
        expectedFile = "src/test/resources/pigunit/output/core/eligibility_rpl/rpl_eligibility.expected";  
    }
    
    @Test
    public void testCoreRplEligibility() throws IOException, ParseException {
        test = new PigTest(pigScript, params, pigServer, cluster);
        (test.getPigServer()).debugOn();
        String actual = readAlias(aliasToCompare);
        String expected = readExpectedFile(expectedFile);
        System.out.println("Actual::" + actual);
        System.out.println("Expected::" + expected );
        //Assert.assertEquals("Verification Failed: ", expected, actual); 
        // Allow Store command to run and store files
        test.unoverride("STORE");
        test.unoverride("DUMP");
        test.runScript();
        test = new PigTest(scriptCombinePartFile(expectedFile));
        test.runScript();
    }

    public static void removeCacheFiles(String cacheFilePath) {
        File f = new File(cacheFilePath);
        for(String fileName: f.list()) {
         (new File(fileName)).delete(); 
        }
    }

    // Helper Methods 
    public static String [] scriptCombinePartFile(String expectedOutputLocation) {
        File f = new File(expectedOutputLocation);
        String [] actualName = (f.getName()).split("\\.");
        String actualOutputLocation = f.getParent() +"/"+ actualName[0]+".actual"; 
        System.out.println("Expected Output at:" + expectedOutputLocation);
        System.out.println("Actual Output at:" + actualOutputLocation);
        String [] mergeFiles = {
            "fs -getmerge build/output/eligibility_rpl/part* " + actualOutputLocation
            //"fs -rm build/output/sbt/part*",
        };
        return mergeFiles;
    }

    public static String readExpectedFile(String fileName) throws IOException, ParseException {
        return StringUtils.join(readFile(new File(fileName)).split("(\\r\\n|\\n)"), "\n");
    }
 
    public static String readAlias(String alias) throws IOException, ParseException {
        Iterator<Tuple> iterator = test.getAlias(alias);
        List<String> actualResults = new ArrayList<String>();
        while (iterator.hasNext()) {
            actualResults.add(iterator.next().toString());
        }
         return StringUtils.join(actualResults, "\n");
    }

    public static String readFile(File file) throws IOException, ParseException {
        FileInputStream stream = new FileInputStream(file);
        try {
          FileChannel fc = stream.getChannel();
          MappedByteBuffer bb = fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size());
          return Charset.defaultCharset().decode(bb).toString();
        }
        finally {
          stream.close();
        }
    }
}
