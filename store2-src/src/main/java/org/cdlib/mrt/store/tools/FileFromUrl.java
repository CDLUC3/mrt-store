/******************************************************************************
Copyright (c) 2005-2012, Regents of the University of California
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
*******************************************************************************/
package org.cdlib.mrt.store.tools;

import org.cdlib.mrt.cloud.action.*;
import java.io.File;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.util.concurrent.Callable;

import org.cdlib.mrt.cloud.utility.StoreMapStates;

import org.cdlib.mrt.core.Manifest;
import org.cdlib.mrt.core.ManifestRowAbs;

import org.cdlib.mrt.core.ComponentContent;
import org.cdlib.mrt.core.FileComponent;
import org.cdlib.mrt.core.Identifier;


import org.cdlib.mrt.store.VersionState;

import org.cdlib.mrt.utility.DateUtil;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.s3.service.CloudStoreInf;
import org.cdlib.mrt.utility.StringUtil;
/**
 * Run fixity
 * @author dloy
 */
public class FileFromUrl
{

    protected static final String NAME = "FileFromUrl";
    protected static final String MESSAGE = NAME + ": ";
    protected static final boolean DEBUG = false;
    protected static final String TMPPREFIX = "tmpxx";

    
    public static File getFile(URL url, LoggerInf logger)
        throws TException
    {
        try {
            if (url == null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "fillComponent - component URL missing");
            }
            File returnFile = null;
            if (url.toString().startsWith("file:/")) {
                returnFile = fileFromFileURL(url);
            } else {
                returnFile = fileFromURL(url, logger);
            }
            return returnFile;


        } catch (TException tex) {
            throw tex;

        } catch (Exception ex) {
            String msg = MESSAGE + "buildCloudComponent - Exception:" + ex;
            logger.logError(msg, 2);
            logger.logError(StringUtil.stackTrace(ex), 10);
            throw new TException.GENERAL_EXCEPTION(msg);
        }

    }
    
    private static File fileFromURL(URL url, LoggerInf logger)
        throws TException
    {
        try {
            if (url == null) {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "fillComponent - component URL missing");
            }
            File tmpFile = FileUtil.getTempFile(TMPPREFIX, ".txt");
            FileUtil.url2File(logger, url, tmpFile);
            return tmpFile;


        } catch (TException tex) {
            throw tex;

        } catch (Exception ex) {
            String msg = MESSAGE + "buildCloudComponent - Exception:" + ex;
            logger.logError(msg, 2);
            logger.logError(StringUtil.stackTrace(ex), 10);
            throw new TException.GENERAL_EXCEPTION(msg);
        }

    }
    
    private static File fileFromFileURL(URL fileURL)
        throws TException
    {
        try {
            if (fileURL == null) {
                throw new TException.INVALID_OR_MISSING_PARM("getFile null");
            }
            if (DEBUG) System.out.println("TEST URL:" + fileURL.toString());
            URI fileURI = fileURL.toURI();
            File bldFile = null;
            try {
                bldFile = new File(fileURI);
            } catch (Exception ex) {
                throw new TException.INVALID_OR_MISSING_PARM("File URL invalid:" + fileURL.toString());
            }
            if (bldFile.exists()) {
                if (DEBUG) System.out.println("File exists");
                return bldFile;
            } else {
                throw new TException.INVALID_OR_MISSING_PARM("File does not exist:" + fileURL.toString());
            }

        } catch (TException tex) {
            System.out.println(tex);
            throw tex;

        } catch (Exception ex) {
            String msg = MESSAGE + "buildCloudComponent - Exception:" + ex;
            ex.printStackTrace();
            throw new TException.GENERAL_EXCEPTION(msg);
        }

    }
    
    public static boolean delete(File testFile)
    {

        try {
            if (testFile != null) {
                if (testFile.getAbsolutePath().contains(TMPPREFIX)) {
                    
                    testFile.delete();
                    if (testFile.exists()) {
                        System.out.println("***Component delete failure:" 
                                + testFile.getAbsolutePath());
                    } else {
                        if (DEBUG) System.out.println("+++Component deleted:" 
                                + testFile.getAbsolutePath());
                        return true;
                    }
                }
            }
            return false;
            
        } catch (Exception ex) { 
            System.out.println("***Component delete exception(" 
                            + testFile.getAbsolutePath() + "):" + ex);
            return false;
        }
            
    }
}

