package org.cdlib.mrt.store.test;

import org.cdlib.mrt.store.can.CAN;
import org.cdlib.mrt.store.can.CANAbs;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

import org.cdlib.mrt.formatter.FormatterInf;
import org.cdlib.mrt.formatter.FormatterAbs;
import org.cdlib.mrt.core.FileComponent;
import org.cdlib.mrt.store.FileState;
import org.cdlib.mrt.store.NodeState;
import org.cdlib.mrt.store.ObjectState;
import org.cdlib.mrt.store.VersionContent;
import org.cdlib.mrt.store.VersionState;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.formatter.ANVLFormatter;
import org.cdlib.mrt.formatter.XMLFormatter;
import org.cdlib.mrt.utility.TFrame;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.TExceptionEnum;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StateInf;
import org.cdlib.mrt.utility.StringUtil;


public class TestFormatterObject
{


    protected static final String NAME = "TestFormatterObject";
    protected static final String MESSAGE = NAME + ": ";
    protected static LoggerInf logger = null;
    protected TFrame mFrame = null;
    protected File stateDir = null;
    protected Identifier objectID = null;
    protected int versionID = -1;
    protected String fileID = null;

    TestFormatterObject(TFrame mFrame)
    {
        this.mFrame = mFrame;
    }

    /**
     * Main method
     */
    public static void main(String args[])
    {
        System.out.println("TestFormatterObject entered");
        try
        {
            String propertyList[] = {
                "resources/TStorageLogger.properties",
                "resources/TFrameTest.properties"};
            TFrame mFrame = new TFrame(propertyList, NAME);
            logger = mFrame.getLogger();
            TestFormatterObject test = new TestFormatterObject(mFrame);
            test.run();

        }  catch(Exception e)  {
            if (logger != null)
            {
                logger.logError(
                    "Main: Encountered exception:" + e, 0);
                logger.logError(
                        StringUtil.stackTrace(e), 10);
            }
        }
    }

    protected void run()
    {
        try {
            String storeName = mFrame.getProperty(NAME + ".store");
            log("storeName=" + storeName, 0);
            File storeFile = new File(storeName);
            if (!storeFile.exists()) {
                log("storeFile does not exist", 0);
            }

            String objectIDS = mFrame.getProperty(NAME + ".objectID");
            objectID = new Identifier(objectIDS);
            log("objectID=" + objectID, 0);

            String versionIDS = mFrame.getProperty(NAME + ".versionID");
            versionID = Integer.parseInt(versionIDS);
            log("versionID=" + versionID, 0);

            fileID = mFrame.getProperty(NAME + ".fileID");
            log("fileID=" + fileID, 0);

            String stateDirS = mFrame.getProperty(NAME + ".stateDir");
            stateDir = new File(stateDirS);
            if (!stateDir.exists()) {
                throw new TException.INVALID_CONFIGURATION(
                        "stateDir not supplied");
            }
            log("stateDir=" + stateDir.getCanonicalPath(), 0);

            int nodeID =0;
            CAN can = CANAbs.getCAN(logger, storeName);
            System.out.println(MESSAGE + "begin processing:");
            testState(can, FormatterInf.Format.xml);
            testState(can, FormatterInf.Format.anvl);
            testState(can, FormatterInf.Format.json);

        }  catch(Exception e)  {
            if (logger != null)
            {
                logger.logError(
                    "Main: Encountered exception:" + e, 0);
                logger.logError(
                        StringUtil.stackTrace(e), 10);
            }
        }
    }

    protected void testState(CAN can, FormatterInf.Format format)
    {
        try {
            String ext = "txt";
            switch (format) {
                case xml: ext = ".XML.xml"; break;
                case anvl: ext = "ANVL.txt"; break;
                case json: ext = "JSON.txt"; break;
            }
            log(MESSAGE + "BEGIN: CANState", 0);
            NodeState nodeState = can.getNodeState();
            formatState(format, nodeState, "nodeState" + ext);

            log(MESSAGE + "BEGIN: versionState", 0);
            VersionState versionState = can.getVersionState(objectID, versionID);
            System.out.println(versionState.dump("***versionState***"));
            formatState(format, versionState, "versionState" + ext);

            log(MESSAGE + "BEGIN: objectState", 0);
            ObjectState objectState = can.getObjectState(objectID);
            log(MESSAGE + "dump objectState:" + objectState.dump(""), 0);
            formatState(format, objectState, "objectState" + ext);

            log(MESSAGE + "BEGIN: fileState", 0);
            FileComponent fileComponent = can.getFileState(objectID, versionID, fileID);
            FileState fileState = new FileState(fileComponent);
            formatState(format, fileState, "fileState" + ext);

            log(MESSAGE + "BEGIN: exceptionState", 0);
            TException exceptionState = new TException.GENERAL_EXCEPTION(
                    "test exception");
            formatState(format, exceptionState, "exceptionState" + ext);

        } catch (Exception ex) {
            log(MESSAGE + "Exception:" + ex, 0);
            log(MESSAGE + "Trace:" + StringUtil.stackTrace(ex), 0);
        }
    }

    protected void formatState(
            FormatterInf.Format format,
            StateInf state,
            String outFileName)
    {
        try {
            File outFile = new File(stateDir, outFileName);
            log(MESSAGE + "formatState - outFile=" + outFile.getCanonicalPath(), 0);
            FileOutputStream outFileStream = new FileOutputStream(outFile);
            PrintStream printStream = new PrintStream(outFileStream, true, "utf-8");
            FormatterInf formatter = FormatterAbs.getFormatter(format, logger);
            formatter.format(state, printStream);
            printStream.close();
            printIt(outFileName, outFile);

        } catch (Exception ex) {
            log(MESSAGE + "Exception:" + ex, 0);
            log(MESSAGE + "Trace:" + StringUtil.stackTrace(ex), 0);
        }
    }

    protected void printIt(String header, File inFile)
            throws Exception
    {
        System.out.println(header);
        FileInputStream fis = new FileInputStream(inFile);
        BufferedReader br = new BufferedReader(new InputStreamReader(fis, "utf-8"));
        String line = null;
        while (true) {
            line = br.readLine();
            if (line == null) break;
            System.out.println(line);
        }
    }


    public static void testGetVersionContent(
            TFrame framework,
            CAN can,
            Identifier objectID)
    {
        try {
            log(MESSAGE + "before testGetVersionContent", 0);
            VersionContent versionContent = can.getVersionContent(objectID, 0);
            dumpState("versionState2",versionContent);
            dumpCANState("CAN State", can);
            String outFileName = framework.getProperty(NAME + ".outFile");
            log("outFileName=" + outFileName, 0);
            FileOutputStream outFileStream = new FileOutputStream(outFileName);
            PrintStream printStream = new PrintStream(outFileStream, true, "utf-8");
            //XMLFormatter formatter = XMLFormatter.getXMLFormatter(logger, outFileStream);
            ANVLFormatter formatter = ANVLFormatter.getANVLFormatter(logger);
            formatter.format(versionContent, printStream);

        } catch (Exception ex) {
            log(MESSAGE + "Exception:" + ex, 0);
            log(MESSAGE + "Trace:" + StringUtil.stackTrace(ex), 0);
        }
    }


    protected static void dumpState(String header, VersionContent versionState)
    {
        log("************dumpState " + header + " ***************", 0);
        LinkedHashMap<String, FileComponent> versionFiles = versionState.getVersionTable();
        Set<String> fileSet = versionFiles.keySet();
        for (String key : fileSet) {
            if (StringUtil.isEmpty(key)) continue;
            FileComponent fileState = versionFiles.get(key);
            if (fileState == null) continue;
            log(MESSAGE + fileState.dump("***" + key + "***:"), 0);
        }

    }

    protected static void dumpCANState(String header, CAN can)
        throws TException
    {
        log("************CANSTATE " + header + " ***************", 0);
        NodeState nodeState = can.getNodeState();
        log(nodeState.dump("*******TestCAN********"), 0);

    }

    protected static void log(String msg, int lvl)
    {
        System.out.println(msg);
        //logger.logMessage(msg, 0, true);
    }

}
