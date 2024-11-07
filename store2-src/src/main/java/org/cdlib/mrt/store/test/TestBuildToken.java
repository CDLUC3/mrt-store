/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.cdlib.mrt.store.test;
import java.io.IOException;
import org.cdlib.mrt.s3.service.NodeIO;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.TFileLogger;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.log.utility.Log4j2Util;
import org.cdlib.mrt.store.fix.BuildTokenCC;
import org.json.JSONObject;
/**
 * 
 *
 * @author replic
 */
public class TestBuildToken 
{
    protected static final String NAME = "TestChangeTokenFix";
    protected static final String MESSAGE = NAME + ": ";
    
    protected NodeIO nodeIO = null;
    protected Long processNode = null;
    protected LoggerInf logger = null;
    protected Boolean exec = null;
    
    protected static String [] testEntries3 = 
    {
        "ucm_lib_elowe|ark:/13030/m5dc576c|3|3|281400",
        "ucm_lib_elowe|ark:/13030/m52p27h8|3|3|632328",
        "ucm_lib_elowe|ark:/13030/m5qz9h96|3|3|900960"
    };
    
    protected static String [] testEntriesOne = 
    {
        "ucm_lib_elowe|ark:/13030/m5dc576c|3|3|281400"
    };
    
    protected static String [] testEntriesTwo = 
    {
        "ucm_lib_elowe|ark:/13030/m52p27h8|3|3|632328",
    };
    
    protected static String [] testEntriesThree = 
    {
        "ucm_lib_elowe|ark:/13030/m5qz9h96|3|3|900960"
    };
    
    public TestBuildToken(NodeIO nodeIO, Long processNode, Boolean exec, LoggerInf logger)
            throws IOException,TException 
    {
        this.nodeIO = nodeIO;
        this.processNode = processNode;
        this.logger = logger;
        this.exec = exec;
    }
 
    public static void main(String[] args) 
            throws IOException,TException 
    {
        try {
            Log4j2Util.setRootLevel("info");
            Log4j2Util.whichLog4j2("start");
            if (false) return;
            //Log4j2Util.setLoggerLevel("FixLog","trace");
            Log4j2Util.setLoggerLevel("FixLog","info");
            //long processNode = 9501;
            long processNode = 7502;
            LoggerInf logger = new TFileLogger(NAME, 50, 50);
            //String yamlName = "jar:nodes-sdsc-temp";
            String yamlName = "yaml:";
            //String collection = "testCollection";
            NodeIO nodeIO = NodeIO.getNodeIOConfig(yamlName, logger) ;
            boolean exec = true;
            TestBuildToken testToken = new TestBuildToken(nodeIO, processNode, exec, logger);
            for (String entry: testEntriesThree) {
                String [] parts = entry.split("\\|", 5);
                if (parts.length != 5) {
                    throw new TException.INVALID_OR_MISSING_PARM("TestBuildToken != 5");
                }
                Identifier objectID = new Identifier(parts[1]);
                String collection = parts[0];
                JSONObject response = testToken.process(processNode, collection, objectID);
                System.out.println("***RESPONSE:\n" + response.toString(2));
            }
            
        } catch (TException tex) {
            System.out.println("TException:" + tex);
            tex.printStackTrace();
            
        } catch (Exception ex) {
            System.out.println("TException:" + ex);
            ex.printStackTrace();
            
        }
    }
 
    public JSONObject process(Long processNode, String collection, Identifier objectID)
            throws IOException,TException 
    {
        JSONObject jsonResponse = new JSONObject();
        try {
            
            System.out.println("PROCESS"
                    + " - processNode:" + processNode
                    + " - collection:" + collection
                    + " - objectID:" + objectID
                    + " - exec:" + exec
                    + "\\n\\n"
            );
            BuildTokenCC buildTokenCC = new BuildTokenCC(
                collection,
                nodeIO, 
                processNode, 
                objectID, 
                exec,
                logger);
            buildTokenCC.process();
            jsonResponse = buildTokenCC.getJsonResponse();
            
            return jsonResponse;
            
        } catch (TException tex) {
            System.out.println("TException:" + tex);
            tex.printStackTrace();
            return jsonResponse;
            
            
        } catch (Exception ex) {
            System.out.println("TException:" + ex);
            ex.printStackTrace();
            return jsonResponse;
        }
    }
    
    
}