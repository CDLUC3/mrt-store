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
import java.util.List;
import java.util.concurrent.Callable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.cdlib.mrt.cloud.VersionMap;
import org.cdlib.mrt.core.Identifier;

import org.cdlib.mrt.log.utility.AddStateEntryGen;


import org.cdlib.mrt.cloud.ManifestSAX;
import org.cdlib.mrt.cloud.VersionData;
import org.cdlib.mrt.store.VersionState;

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
public class LogEntryVersion
{

    protected static final String NAME = "LogEntryVersion";
    protected static final String MESSAGE = NAME + ": ";
    private static final Logger log4j = LogManager.getLogger();
    
    protected String serviceProcess = null;
    protected Long duration = null;
    protected Long node = null;
    protected VersionState versionState = null;
    protected Long addBytes = null;
    protected Long addFiles = null;
    protected AddStateEntryGen entry = null;
    
    public static LogEntryVersion getLogEntryVersion(
            String keyPrefix,
            String serviceProcess,
            Long node,
            Long duration,
            VersionState versionState)
    
        throws TException
    {
        return new LogEntryVersion(keyPrefix, serviceProcess, node, duration, versionState);
    }
    
    public LogEntryVersion(
            String keyPrefix, String serviceProcess, Long node, Long duration, VersionState versionState)
        throws TException
    {
        if (StringUtil.isAllBlank(keyPrefix)) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "keyPrefix missing");
        }
        if (StringUtil.isAllBlank(serviceProcess)) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "serviceProcess missing");
        }
        if (node == null) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "node missing");
        }
        if (duration == null) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "duration missing");
        }
        if (versionState == null) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + "versionState missing");
        }
        this.serviceProcess = serviceProcess;
        this.node = node;
        this.duration = duration;
        this.versionState = versionState;
        entry = AddStateEntryGen.getAddStateEntryGen(keyPrefix, "storage", serviceProcess);
        setEntry();
    }
    
    private void setEntry()
        throws TException
    {
        entry.setObjectID(versionState.getObjectID());
        entry.setVersion(versionState.getIdentifier());
        entry.setTargetNode(node);
        entry.setDurationMs(duration);
        entry.setBytes(versionState.getDeltaSize());
        entry.setFiles(versionState.getDeltaNumFiles());
        entry.setVersions(1);
        entry.setCurrentVersion(versionState.getIdentifier());
        log4j.debug("LogEntryVersion entry built");
    }
    
    public void addEntry()
        throws TException
    {
        entry.addLogStateEntry("StoreJSON");
    }
}

