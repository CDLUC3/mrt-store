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
package org.cdlib.mrt.store.zoo;

import org.cdlib.mrt.store.*;
import org.cdlib.mrt.utility.StringUtil;

/**
 * Specifies the httpName and zooName used in flag processing
 * @author dloy
 */
public enum ZooFlagsEnum {

    access_queue("access-queue", "access/large-queue"),
    pause_ingest("pause-ingest", "ingest/pause");
    protected String httpName = null;
    protected String zooPath = null;

    ZooFlagsEnum(String httpName, String zooPath)
    {
        this.httpName = httpName;
        this.zooPath = zooPath;
    }

    public String getHttpName() {
        return httpName;
    }

    public String getZooPath() {
        return zooPath;
    }

    /**
     * Match the Storage type to type and description
     * @param type storage type
     * @param name storage description
     * @return enumerated StorageType value
     */
    public static ZooFlagsEnum valueOf(String httpName, String zooPath)
    {
        boolean DEBUG = false;
            if (DEBUG) System.out.println("StorageTypesEnum"
                    + " - type=" + httpName
                    + " - zooName=" + zooPath
                    );
        if (StringUtil.isEmpty(httpName) || StringUtil.isEmpty(zooPath)) return null;
        
        httpName = httpName.toLowerCase();
        zooPath = zooPath;
        for (ZooFlagsEnum p : ZooFlagsEnum.values()) {
            if (DEBUG) System.out.println("ZooFlagsEnum"
                    + " - p.getHttpName()=" + p.getHttpName()
                    + " - p.getZooName()=" + p.getZooPath()
                    );
            if (p.getHttpName().equals(httpName) && p.getZooPath().equals(zooPath)) {
                return p;
            }
        }
        return null;
    }
    
    public static ZooFlagsEnum valueOfZooPath(String httpName)
    {
        boolean DEBUG = false;
            if (DEBUG) System.out.println("ZooFlagsEnum.ZooFlagsEnum"
                    + " - zooName=" + httpName
                    );
        if (StringUtil.isEmpty(httpName) ) return null;
        httpName = httpName.toLowerCase();
        for (ZooFlagsEnum p : ZooFlagsEnum.values()) {
            if (DEBUG) System.out.println("ZooFlagsEnum"
                    + " - p.getHttpName()=" + p.getHttpName()
                    + " - p.getZooPath()=" + p.getZooPath()                    );
            if (p.getHttpName().equals(httpName)) {
                return p;
            }
        }
        return null;
    }
}
