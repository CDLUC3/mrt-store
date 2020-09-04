/*
Copyright (c) 2005-2009, Regents of the University of California
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

- Redistributions of source code must retain the above copyright notice,
  this list of conditions and the following disclaimer.
- Redistributions in binary form must reproduce the above copyright
  notice, this list of conditions and the following disclaimer in the
  documentation and/or other materials provided with the distribution.
- Neither the name of the University of California nor the names of its
  contributors may be used to endorse or promote products derived from
  this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
OF THE POSSIBILITY OF SUCH DAMAGE.
*********************************************************************/
package org.cdlib.mrt.store.test;

import java.io.File;
import java.io.InputStream;

import org.cdlib.mrt.utility.URLEncoder;
import java.util.Properties;
import java.util.Vector;

import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.store.app.jersey.KeyNameHttpInf;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.HTTPUtil;
import org.cdlib.mrt.utility.ListProcessor;
import org.cdlib.mrt.utility.ListProcessorThreads;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.TFrame;

/**
 * Run a Storage load test
 *
 * @author dloy
 */

public class StoreLoadTest
        extends ListProcessorThreads
        implements ListProcessor
{

    public enum Tests {
        test1,test2
    }
    protected String NAME = "StoreLoadTest";
    protected String MESSAGE = NAME + ":";
    public static final String LS =  System.getProperty("line.separator");
    public static final int DEFAULTDELTA = 25;

    Vector<String> manifestURLs = new Vector<String>(10);
    Vector<Integer> addSizes = new Vector<Integer>(10);
    protected int delta = 0;
    protected int nodeID = -1;
    protected String mrtURL = null;
    protected File statsDir = null;
    protected Tests runTest = Tests.test1;

    /**
     * Main method
     */
    public static void main(String args[])
    {
        TFrame framework = null;
        try
        {
            framework = new TFrame(args, "StoreLoadTest");
            
            // Create an instance of this object
            StoreLoadTest test = new StoreLoadTest(framework);
            test.run();
        }
        catch(Exception e)
        {
            if (framework != null)
            {
                framework.getLogger().logError(
                    "Main: Encountered exception:" + e, 0);
                framework.getLogger().logError(
                        StringUtil.stackTrace(e), 10);
            }
        }
    }
    
    public StoreLoadTest(TFrame framework)
        throws TException
    {
        super(framework);
        initialize(framework);
    }
    
    private void initialize(TFrame fw)
        throws TException
    {

        try {
            Properties clientProperties = getClientProperties();
            for (int i=1; true; i++) {
                String manifest = clientProperties.getProperty("manifest." + i);
                if (manifest == null) break;
                manifestURLs.add(manifest);
                String addSizeS  = clientProperties.getProperty("size." + i);
                int addSize = Integer.parseInt(addSizeS);
                addSizes.add(addSize);
            }
            if (manifestURLs.size() == 0) {
                throw new TException.INVALID_OR_MISSING_PARM(
                        MESSAGE + "Missing manifestURLs");
            }
            String nodeS = getRequired(clientProperties, "nodeID");
            nodeID = Integer.parseInt(nodeS);
            mrtURL = getRequired(clientProperties, "mrtURL");
            System.out.println(PropertiesUtil.dumpProperties(NAME, clientProperties, 20));
            String statsDirS = clientProperties.getProperty("statsDir");
            if (statsDirS == null) {
                throw new TException.INVALID_OR_MISSING_PARM(
                        MESSAGE + "Missing statsDir");
            }

            statsDir = new File(statsDirS);
            if (!statsDir.exists()) {
                throw new TException.INVALID_OR_MISSING_PARM(
                    MESSAGE + "statsDir file not found:" + statsDirS);
            } else {
                String propS = PropertiesUtil.buildLoadProperties(clientProperties);
                File propFile = new File(statsDir, "runprops.txt");
                FileUtil.string2File(propFile, propS);
            }

            String deltaS = clientProperties.getProperty("delta");
            if (StringUtil.isEmpty(deltaS)) {
                delta = DEFAULTDELTA;
            } else {
                delta = Integer.parseInt(deltaS);
            }

            String runTestS = clientProperties.getProperty("runTest");
            if (StringUtil.isNotEmpty(runTestS)) {
                runTestS = runTestS.toLowerCase();
                try {
                    runTest = Tests.valueOf(runTestS);
                } catch (Exception ex) {
                    runTest = Tests.test1;
                }
            }
            System.out.println("Run test:" + runTest.toString());
        } catch (Exception ex) {
            throw new TException.INVALID_OR_MISSING_PARM(
                    MESSAGE + "Unable to load db manager: Exception:" + ex);
        }
    }
        

    
    /**
     * Name of EnrichmentList
     * @return name to be applied for identifying process properties
     */
    @Override
    public String getName() 
    {
        return "StoreLoadTest";
    }

    /**
     * <pre>
     * Process an incoming line which contains
     * 1) ark
     * 2) sequence number
     * Use the ark for issuing an addVersion to a storage service
     * </pre>
     * @param line line containing ark and sequence number
     * @param processProp runtime properties
     * @throws TException process exception
     */
    @Override
    public void process(
            String line,
            Properties processProp)
	throws TException
    {
        try {
            String[] items = line.split("\\s+");
            if ((items == null) || (items.length != 2)) {
                throw new TException.INVALID_DATA_FORMAT(
                        MESSAGE + "entry format invalid: line=" + line
                        + " - linecnt=" + items.length);
            }
            String objectIDS = items[0];
            String seqS = items[1];
            Identifier objectID = new Identifier(objectIDS, Identifier.Namespace.ARK);
            long seq = Long.parseLong(seqS);
            long rem = seq % delta;
            switch (runTest) {
                case test1: addTest1(seq, objectID, rem); break;
                case test2: addTest2(seq, objectID, rem); break;
                default: throw new TException.INVALID_DATA_FORMAT ("Unknown test");
            }
            if (rem == 0) {
                m_status.bump("z-" + seq + "-time", m_status.getCount("time"));
                m_status.bump("z-" + seq + "-cnt", m_status.getCount("cnt"));
                m_status.bump("z-" + seq + "-size", m_status.getCount("size"));

                File statsFile = new File(statsDir, "stats-" + seq + ".txt");
                FileUtil.string2File(statsFile, m_status.dumpProp());

                outputNodeState(seq);
                String msg = "Status(" + seq + "):" + m_status.dump();
                logger.logMessage(msg, 0);
                System.out.println("Status(" + seq + "):" + m_status.dump());
            }

        }  catch(TException fex){
            m_framework.getLogger().logError(
                MESSAGE + " Exception: " + fex,
                3);
            m_status.bump(line + ".fail");
            throw fex;

        } catch(Exception ex){
            m_framework.getLogger().logError(
                MESSAGE + " Exception: " + ex,
                3);
            m_framework.getLogger().logError(
                MESSAGE + " StackTrace: " + StringUtil.stackTrace(ex), 10);
            m_status.bump(line + ".fail");
            throw new TException.GENERAL_EXCEPTION(MESSAGE + " Exception: " + ex);
        }
    }

    /**
     * Write out a NodeState as an xml file
     * @param itemNum - sequence number from list
     * @throws TException
     */
    protected void outputNodeState(long itemNum)
        throws TException
    {
        InputStream  response = null;
        String requestURL = null;
        try {
            requestURL = mrtURL + "/nodeState/" + nodeID
                + "?" + KeyNameHttpInf.RESPONSEFORM + "=xml";
            String fileName = null;
            if (itemNum > 0) {
                fileName = "node-" + itemNum + ".xml";
            } else {
                fileName = "node-final.xml";
            }
            File nodeFile = new File(statsDir, fileName);
            response = HTTPUtil.getObject(requestURL, 60000, 3);
            FileUtil.stream2File(response, nodeFile);
         
        } catch (TException fe) {
            throw fe;
            
        } catch (Exception ex) {
            logger.logError(
                MESSAGE + "outputNodeState - Failed to output node state:" + requestURL + " - Exception:" + ex, 0);
            logger.logError("stackTrace:" + StringUtil.stackTrace(ex), 0);
            throw new TException.GENERAL_EXCEPTION(
                ex.getMessage());

        } finally {
            try {
                if (response != null) {
                    response.close();
                }
            } catch (Exception fex) {}
        }
    }

    @Override
    protected void processCompletion()
    {
        try {
            File statsFile = new File(statsDir, "stats-final.txt");
            FileUtil.string2File(statsFile, m_status.dumpProp());
            outputNodeState(0);
            
        } catch (Exception ex) {}
    }
    /**
     * add test1 rotates small through list of manifests
     * @param itemNum process sequence value
     * @param objectID object identifier
     * @return process result
     * @throws TException
     */
    protected String addTest1(long itemNum, Identifier objectID, long rem)
        throws TException
    {
            long base = manifestURLs.size();
            int manInx = (int)(itemNum % base);
            return addTestA(itemNum, rem, manInx, objectID);
    }

    /**
     * add test1 rotates small through list of manifests
     * @param itemNum process sequence value
     * @param objectID object identifier
     * @return process result
     * @throws TException
     */
    protected String addTest2(long itemNum, Identifier objectID, long rem)
        throws TException
    {
            long base = manifestURLs.size();
            int manInx = (int)((itemNum + 1) % base);
            return addTestA(itemNum, rem, manInx, objectID);
    }

    /**
     * add test1 rotates small through list of manifests
     * @param itemNum process sequence value
     * @param objectID object identifier
     * @return process result
     * @throws TException
     */
    protected String addTestA(long itemNum, long rem, int manInx, Identifier objectID)
        throws TException
    {
        String path = null;

        try {
            long base = manifestURLs.size();
            long addSize = addSizes.get(manInx);
            String manifestURL = manifestURLs.get(manInx);
            String requestURL = mrtURL + "/addVersion/" + nodeID
                    + "/" + URLEncoder.encode(objectID.getValue(), "utf-8");
            Properties prop = new Properties();
            prop.setProperty("url", manifestURL);
            Long startTime = m_status.getTime();
            InputStream  response = HTTPUtil.postObject(requestURL, prop, 120000);
            String result = StringUtil.streamToString(response, "utf-8");
            long diffTime = m_status.getTime() - startTime;
            m_status.bump("cnt");
            m_status.bump("cnt-m" + manInx);
            m_status.bump("time", diffTime);
            m_status.bump("time-m" + manInx, diffTime);
            m_status.bump("size", addSize);
            m_status.bump("size-m" + manInx, addSize);

            if (rem == 0) {
                for (int mi=0; mi < base; mi++) {
                    m_status.bump("z-" + itemNum + "-time-m" + mi, m_status.getCount("time-m" + mi));
                    m_status.bump("z-" + itemNum + "-cnt-m" + mi, m_status.getCount("cnt-m" + mi));
                    m_status.bump("z-" + itemNum + "-size-m" + mi, m_status.getCount("size-m" + mi));
                }
            }

            return result;

        } catch (TException fe) {
            logger.logError(
                MESSAGE + "queueProcess - Failed to get object path= " + path + " - Exception:" + fe, 0);
            throw fe;

        }  catch (Exception ex) {
            logger.logError(MESSAGE + "Exception for " + path + ":" + ex, 0);
            logger.logError(MESSAGE + "trace:" + StringUtil.stackTrace(ex), 10);
            throw new TException.GENERAL_EXCEPTION(
                ex.getMessage());
        }
    }


    /**
     * Throw exception if no value is found
     * @param prop Properties to test
     * @param key  lookup key
     * @throws TException thown if invalid parms or no value found
     */
    protected String getRequired(Properties prop, String key)
        throws TException
    {
        String retValue = null;
        if ((prop == null) || StringUtil.isEmpty(key)) {
            throw new TException.INVALID_OR_MISSING_PARM(
                    MESSAGE + "required - missing required parameter");
        }
        retValue = prop.getProperty(key);
        if (StringUtil.isEmpty(retValue)) {
            throw new TException.INVALID_OR_MISSING_PARM("Required missing parm:" + key);
        }
        return retValue;
    }
}
