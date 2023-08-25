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
package org.cdlib.mrt.store.logging;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.Callable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.cdlib.mrt.cloud.VersionMap;
import org.cdlib.mrt.core.Identifier;

import org.cdlib.mrt.log.utility.AddStateEntryGen;


import org.cdlib.mrt.cloud.ManifestSAX;
import org.cdlib.mrt.cloud.VersionData;
import org.cdlib.mrt.store.TokenStatus;

import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.store.NodeInf;
import org.cdlib.mrt.utility.DateUtil;
import org.cdlib.mrt.utility.StringUtil;
/**
 * Run fixity
 * @author dloy
 */
public class LogEntryTokenStatus
{

    protected static final String NAME = "LogEntryTokenStatus";
    protected static final String MESSAGE = NAME + ": ";
    private static final Logger log4j = LogManager.getLogger();
    
    protected String serviceProcess = "AsyncToken";
    protected TokenStatus tokenStatus = null;
    protected AddStateEntryGen entry = null;
    
    public static LogEntryTokenStatus getLogEntryTokenStatus(
            TokenStatus tokenStatus)
        throws TException
    {
        return new LogEntryTokenStatus(tokenStatus);
    }
    
    public LogEntryTokenStatus(TokenStatus tokenStatus)
        throws TException
    {
        if (StringUtil.isAllBlank(serviceProcess)) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "serviceProcess missing");
        }
        if (tokenStatus == null) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "tokenStatus missing");
        }
        this.serviceProcess = serviceProcess;
        this.tokenStatus = tokenStatus;
        entry = AddStateEntryGen.getAddStateEntryGen("token", "storage", serviceProcess);
        log4j.debug("LogEntryTokenStatus constructor");
        setEntry();
    }
    
    private void setEntry()
        throws TException
    {
        entry.setObjectID(tokenStatus.getObjectID());
        entry.setVersion(tokenStatus.getVersionID());
        entry.setSourceNode(tokenStatus.getExtractNode());
        Long buildStart = tokenStatus.getBuildStartMil();
        Long buildEnd  = tokenStatus.getBuildEndMil();
        if ((buildEnd != null) && (buildStart != null)) {
            entry.setDurationMs(buildEnd - buildStart);
        }
        entry.setVersions(1);
        entry.setBytes(tokenStatus.getCloudContentBytes());
        entry.setKey(tokenStatus.getToken());
        entry.setFiles(tokenStatus.getBuildFileCnt());
        setProperties();
        
        log4j.debug("LogEntryTokenStatus entry built");
    }
    private void setProperties()
        throws TException
    {
        Properties prop = new Properties();
        prop.setProperty("archiveType", tokenStatus.getArchiveType().toString());
        prop.setProperty("archiveContent", tokenStatus.getArchiveContent().toString());
        entry.setProperties(prop);
        log4j.debug("LogEntryTokenStatus properties added");
    }
    
    
    
    public void addEntry()
        throws TException
    {
        entry.addLogStateEntry("StoreJSON");
    }
}

