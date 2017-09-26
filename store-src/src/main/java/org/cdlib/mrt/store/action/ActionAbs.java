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
package org.cdlib.mrt.store.action;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.PrintStream;
import java.sql.Connection;
import java.util.Properties;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;

import org.cdlib.mrt.utility.FixityTests;
import org.cdlib.mrt.cloud.ManInfo;
import org.cdlib.mrt.cloud.VersionMap;
import org.cdlib.mrt.core.ComponentContent;
import org.cdlib.mrt.core.FileComponent;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.core.MessageDigest;
import org.cdlib.mrt.s3.service.CloudResponse;
import org.cdlib.mrt.s3.service.CloudStoreInf;
import org.cdlib.mrt.s3.service.CloudUtil;


import org.cdlib.mrt.formatter.FormatterAbs;
import org.cdlib.mrt.formatter.FormatterInf;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StateInf;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;
/**
 * Abstract for performing a fixity test
 * @author dloy
 */
public class ActionAbs
{

    protected static final String NAME = "ActionAbs";
    protected static final String MESSAGE = NAME + ": ";
    protected static final String  STATUS_PROCESSING = "processing";
    private static final boolean DEBUG = false;

    protected LoggerInf logger = null;
    protected Exception exception = null;


    public ActionAbs(LoggerInf logger)
    {
        this.logger = logger;
    }
    
    protected void log(String msg, int lvl)
    {
        //System.out.println(msg);
        logger.logMessage(msg, lvl, true);
    }
    
    /**
     * create TException and do appropriate logger
     * @param logger process logger
     * @param msg error message
     * @param ex encountered exception to convert
     * @return TException
     */
    public static TException makeGeneralTException(
            LoggerInf logger,
            String msg,
            Exception ex)
    {
        logger.logError(msg + ex,
                LoggerInf.LogLevel.UPDATE_EXCEPTION);
        logger.logError(msg + " - trace:"
                + StringUtil.stackTrace(ex),
                LoggerInf.LogLevel.DEBUG);
        return new TException.GENERAL_EXCEPTION(
                msg +  "Exception:" + ex);
    }

    public Exception getException() {
        return exception;
    }

    public void setException(Exception exception) {
        this.exception = exception;
    }
    
    public void log(String msg)
    {
        if (DEBUG) System.out.println(msg);
        logger.logMessage(MESSAGE + msg, 10);
    }


    public void throwEx()
        throws TException
    {
        if (exception != null) {
            if (exception instanceof TException) {
                throw (TException) exception;
            } else {
                logger.logError("Exception:" + exception.toString(), 2);
                logger.logError("Trace:" + StringUtil.stackTrace(exception), 10);
                throw new TException(exception);
            }
        }
    }
    
    public static boolean isTempManifestFile(File inFile)
        throws TException
    {
        boolean isTemp = false;
        try {
            String path = inFile.getAbsolutePath();
            //if (path.matches(".*\\/tmp[0123456789]+\\.txt")) return true;
            if (path.matches(".*\\/tmpman\\.[0123456789]+.*\\.xml")) {
                isTemp = true;
            } else {
                isTemp = false;
            }
            if (DEBUG) System.out.println("isTempManifestFile(: " + path + " - isTemp=" + isTemp);
            return isTemp;
        } catch (Exception ex) {
            throw new TException(ex);
        }
    }
    
}

