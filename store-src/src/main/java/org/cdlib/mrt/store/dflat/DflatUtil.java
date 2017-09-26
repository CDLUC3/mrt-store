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
package org.cdlib.mrt.store.dflat;
import java.io.InputStream;

import org.cdlib.mrt.store.VersionContent;
import org.cdlib.mrt.core.Manifest;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StringUtil;
/**
 * Dflat utility routines
 * @author dloy
 */
public class DflatUtil
{
    protected static final boolean debugDump = false;

    /**
     * From a post manifest InputStream
     * Return a VersionContent object
     * @param logger process logger
     * @param manifest post type manifest
     * @param manifestInputStream InputStream to manifest
     * @return VersionContent object defining a file content data
     * @throws TException process excepton
     */
    public static VersionContent getVersionContent(
            LoggerInf logger,
            Manifest manifest,
            InputStream manifestInputStream)
        throws TException
    {
        return new VersionContent(logger, manifest, manifestInputStream);
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

}
