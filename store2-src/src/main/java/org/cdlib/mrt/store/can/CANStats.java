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
package org.cdlib.mrt.store.can;

import java.io.File;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.TExceptionEnum;
import org.cdlib.mrt.utility.TallyTable;

/**
 * Tally stats information for the CAN
 * @author dloy
 */
public class CANStats
        extends TallyTable
{

    protected static final String NAME = "CANStats";
    protected static final String MESSAGE = NAME + ": ";

    public static final String OBJECTCNT = "Object-count";
    public static final String VERSIONCNT = "Version-count";
    public static final String FILECNT = "File-count";
    public static final String TOTALSIZE = "Total-size";
    public static final String COMPONENTFILECNT = "Component-File-count";
    public static final String COMPONENTTOTALSIZE = "Component-Total-size";

    /**
     * Constructor
     * @param loadFile properties file containing tally data for initialization
     * @throws TException process exception
     */
    public CANStats(File loadFile)
            throws TException
    {
        super();
        if (loadFile == null) {
            throw new TException.INVALID_OR_MISSING_PARM (
                     MESSAGE + " load file is missing");
        }
        if (!loadFile.exists()) return;
        loadTable(loadFile);
    }

}
