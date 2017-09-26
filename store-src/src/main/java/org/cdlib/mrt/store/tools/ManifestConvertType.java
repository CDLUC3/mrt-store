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
**********************************************************/
package org.cdlib.mrt.store.tools;

import org.cdlib.mrt.core.*;
import org.cdlib.mrt.core.FileComponent;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Enumeration;
import java.util.Properties;


import org.cdlib.mrt.core.ManifestRowInf;
import org.cdlib.mrt.core.ManifestRowAbs;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.FixityTests;
import org.cdlib.mrt.utility.TFrame;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.TExceptionEnum;
import org.cdlib.mrt.utility.TRuntimeException;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.PropertiesUtil;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.URLEncoder;
/**
 * Generalized line based processor where each line is some
 * functional unit.
 * Routine supports the reading and writing of manifest files
 * @author  David Loy
 */
public class ManifestConvertType 
{
    protected static final String NAME = "ManifestConvert";
    protected static final String MESSAGE = NAME + ": ";

    
    /**
     * Main method
     */
    public static void main(String args[])
    {
        TFrame framework = null;
        try
        {
            String propertyList[] = {
                "testresources/TestLocal.properties"};

            framework = new TFrame(propertyList, NAME);
            String inFileName = framework.getProperty(NAME + ".inFileName");
            File inFile = new File(inFileName);
            String outFileName = framework.getProperty(NAME + ".outFileName");
            File outFile = new File(outFileName);
            String storeBase = framework.getProperty(NAME + ".storeBase");
            LoggerInf logger = framework.getLogger();
            convert(inFile, outFile, storeBase, logger);
            
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

    protected static void convert(File inFile, File outFile, String storeBase, LoggerInf logger)
        throws Exception
    {
        try {
        Manifest manIn = Manifest.getManifest(logger, ManifestRowAbs.ManifestType.object);
        Manifest manOut = Manifest.getManifest(logger, ManifestRowAbs.ManifestType.add);
        manOut.openOutput(outFile);
        ManifestRowObject manRowIn = null;
        ManifestRowAdd manRowOut
                = (ManifestRowAdd)ManifestRowAbs.getManifestRow(ManifestRowAbs.ManifestType.add, logger);
        Enumeration en = manIn.getRows(inFile);
        while (en.hasMoreElements()) {
            manRowIn = (ManifestRowObject)en.nextElement();
            FileComponent fileComponent = manRowIn.getFileComponent();
            addURL(fileComponent, storeBase);
            manRowOut.setFileComponent(fileComponent);
            manOut.write(manRowOut);
            logger.logMessage(fileComponent.dump(MESSAGE), 0);
        }
        manOut.writeEOF();
        manOut.closeOutput();

        } catch (Exception ex) {
            System.out.println("trace:" + StringUtil.stackTrace(ex));
        }
    }
    
    protected static void addURL(FileComponent fileComponent, String storeBase)
        throws TException
    {
        try {
            String fileName = fileComponent.getIdentifier();
            String fileURL = storeBase
                        + "/" + URLEncoder.encode(fileName, "utf-8");
            fileComponent.setURL(fileURL);
            
        } catch (Exception ex) {
            throw new TException(ex);
        }
    }
}
