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
package org.cdlib.mrt.store;

import org.cdlib.mrt.store.cloud.DummyCloudLocation;
import org.cdlib.mrt.store.pairtree.PairTree;
import java.io.File;



import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.TExceptionEnum;

/**
 * ObjectLocation Factory
 * @author dloy
 */
public abstract class ObjectLocationAbs
{
    protected static final String NAME = "ObjectLocationFactory";
    protected static final String MESSAGE = NAME + ": ";

    protected LoggerInf m_logger = null;
    protected int m_nodeID = 0;
    protected File m_storeFile = null;


    /**
     * Constructor
     * @param logger process logger
     * @param storeFile store file base for directory hierarchy
     */
    protected ObjectLocationAbs(
            LoggerInf logger,
            File storeFile)
    {
        this.m_logger = logger;
        this.m_storeFile = storeFile;
    }

    /**
     * Factory - get PairTree
     * @param logger process logger
     * @param storeFile store file base for directory hierarchy
     * @return PairTree handler
     * @throws TException
     */
    public static ObjectLocationInf getPairTree(
            LoggerInf logger,
            File storeFile)
            throws TException
    {
        try {
            if ((storeFile == null) || !storeFile.exists()) {
                throw new TException.INVALID_OR_MISSING_PARM(
                    "File Path not resoved");
            }
        } catch (Exception ex) {
            throw new TException.INVALID_OR_MISSING_PARM(
                    "Unable to resolve ObjectStore base: Exception" + ex);
        }
        try{
            return new PairTree(logger, storeFile);

        } catch (Exception ex) {
                throw new TException.INVALID_OR_MISSING_PARM(
                    "Unable to instantiate PairTree: Exception" + ex);
        }
    }

    /**
     * Factory - get PairTree
     * @param logger process logger
     * @param storeFile store file base for directory hierarchy
     * @return PairTree handler
     * @throws TException
     */
    public static ObjectLocationInf getDummyCloud(
            LoggerInf logger,
            File storeFile)
            throws TException
    {
        try {
            if ((storeFile == null) || !storeFile.exists()) {
                throw new TException.INVALID_OR_MISSING_PARM(
                    "File Path not resoved");
            }
        } catch (Exception ex) {
            throw new TException.INVALID_OR_MISSING_PARM(
                    "Unable to resolve ObjectStore base: Exception" + ex);
        }
        try{
            return new DummyCloudLocation(logger, storeFile);

        } catch (Exception ex) {
                throw new TException.INVALID_OR_MISSING_PARM(
                    "Unable to instantiate PairTree: Exception" + ex);
        }
    }

    /**
     * log message
     * @param msg log message
     * @param lvl verbosity level
     */
    protected void log(String msg, int lvl)
    {
        //System.out.println(msg);
        m_logger.logMessage(msg, 0, true);
    }

}

