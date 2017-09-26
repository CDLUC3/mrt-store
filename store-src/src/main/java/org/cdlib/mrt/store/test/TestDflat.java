/*
Copyright (c) 2005-2010, Regents of the University of California
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:
 *
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

import org.cdlib.mrt.store.dflat.Dflat_1d0;
import org.cdlib.mrt.store.ObjectStoreInf;
import org.cdlib.mrt.store.ObjectStoreAbs;
import java.io.File;

import org.cdlib.mrt.store.VersionState;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.utility.TFrame;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StringUtil;


/**
 *
 * @author dloy
 */
public class TestDflat
{


    protected static final String NAME = "TestDflat";
    protected static final String MESSAGE = NAME + ": ";
    protected static LoggerInf logger = null;

    /**
     * Main method
     */
    public static void main(String args[])
    {
        TFrame framework = null;
        try
        {
            String propertyList[] = {
                "resources/MFrameDefault.properties",
                "resources/MFrameService.properties",
                "resources/FeederService.properties",
                "resources/IngestClient.properties",
                "resources/FeederClient.properties",
                "resources/MFrameLocal.properties"};

            framework = new TFrame(propertyList, NAME);
            logger = framework.getLogger();
            String manifestName = framework.getProperty(NAME + ".manifestName");
            log("manifest=" + manifestName, 0);
            File manifestFile = new File(manifestName);

            String storeName = framework.getProperty(NAME + ".store");
            log("storeName=" + storeName, 0);
            File storeFile = new File(storeName);
            if (!storeFile.exists()) {
                log("storeFile does not exist", 0);
            }

            String objectIDS = framework.getProperty(NAME + ".objectID");
            log("objectIDS=" + objectIDS, 0);
            Dflat_1d0 dflat = ObjectStoreAbs.getDflat_1d0(logger);
            Identifier objectID = new Identifier(objectIDS);
            testPutVersion(dflat, storeFile, objectID, manifestFile);

        }  catch(Exception e)  {
            if (framework != null)
            {
                framework.getLogger().logError(
                    "Main: Encountered exception:" + e, 0);
                framework.getLogger().logError(
                        StringUtil.stackTrace(e), 10);
            }
        }
    }

    public static void testPutVersion(
            ObjectStoreInf dflat,
            File storeFile,
            Identifier objectID,
            File manifestFile)
    {
        try {
            log(MESSAGE + "before ddd testPutVersion", 0);
            VersionState versionState = dflat.addVersion (storeFile, objectID, manifestFile);
            dumpState("TestDflat", versionState);

        } catch (TException ex) {
            log(MESSAGE + "Exception:" + ex, 0);
            log(MESSAGE + "Trace:" + StringUtil.stackTrace(ex), 0);
        }
    }

    protected static void dumpState(String header, VersionState versionState)
    {
        log(MESSAGE + versionState.dump("************dumpState " + header + " ***************"), 0);

    }

    protected static void log(String msg, int lvl)
    {
        System.out.println(msg);
        logger.logMessage(msg, 0, true);
    }

}