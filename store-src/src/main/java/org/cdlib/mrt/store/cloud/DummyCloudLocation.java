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
package org.cdlib.mrt.store.cloud;
import org.cdlib.mrt.store.pairtree.*;

import org.cdlib.mrt.store.ObjectLocationInf;
import org.cdlib.mrt.store.ObjectLocationAbs;
import java.io.File;

import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.store.SpecScheme;
import org.cdlib.mrt.utility.PairtreeUtil;


/**
 * Cloud storage is not file based so dummy operations are required
 * @author dloy
 */
public class DummyCloudLocation
        extends ObjectLocationAbs
        implements ObjectLocationInf
{
    protected static final String NAME = "PairTree";
    protected static final String MESSAGE = NAME + ": ";
    protected File m_root = null;
    protected SpecScheme namasteSpec
            = new SpecScheme(SpecScheme.Enum.pairtree_1d0, null);

    /**
     * Constructor
     * @param logger process logger
     * @param storeFile storage node directory
     */
    public DummyCloudLocation (LoggerInf logger,
            File storeFile)
        throws TException
    {
        super(logger, storeFile);
        File root = new File(storeFile, "cloud");
        if (!root.exists()) {
            root.mkdir();
        }
        m_root = root;
    }

    @Override
    public File buildObjectLocation (Identifier objectID)
            throws TException
    {
        return m_root;
    }

    @Override
    public File getObjectLocation (Identifier objectID)
            throws TException
    {
        if (!m_root.exists()) {
            m_root.mkdir();
        }
        return m_root;
    }

    @Override
    public boolean removeObjectLocation (Identifier objectID)
            throws TException
    {
        return true;
    }

    @Override
    public boolean objectExists (Identifier objectID)
            throws TException
    {
        return true;
    }

    public SpecScheme getNamasteSpec()
    {
        return namasteSpec;
    }
}