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
import java.io.Serializable;
import java.net.URL;
import java.util.Date;
import java.util.ArrayList;
import org.cdlib.mrt.core.DateState;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.store.tools.StoreUtil;
import org.cdlib.mrt.utility.LinkedHashList;
import org.cdlib.mrt.utility.StateInf;

/**
 * State information for Object
 * @author dloy
 */
public class ZooTokenState
        implements StateInf
{
    protected String zooConnectionString = null;
    protected String zooNodeBase= null;
    protected String zooFlagPath = null; 
    protected String zooProcess = null;
    protected Boolean tokenStatus = null;
    protected DateState lockCreated = null;
    protected DateState runDate = new DateState();
    protected String data = null;
    
    public ZooTokenState(
            String zooConnectionString,
            String zooNodeBase,
            String zooProcess,
            String zooFlagPath,
            Boolean tokenStatus)
    {
        this.zooConnectionString = zooConnectionString;
        this.zooNodeBase = zooNodeBase;
        this.zooProcess = zooProcess;
        this.zooFlagPath = zooFlagPath;
        this.tokenStatus = tokenStatus;
    }

    public String getZooConnectionString() {
        return zooConnectionString;
    }

    public void setZooConnectionString(String zooConnectionString) {
        this.zooConnectionString = zooConnectionString;
    }
    
    

    public Boolean getTokenStatus() {
        return tokenStatus;
    }

    public void setTokenStatus(Boolean tokenStatus) {
        this.tokenStatus = tokenStatus;
    }

    public DateState getLockCreated() {
        return lockCreated;
    }

    public void setLockCreated(DateState lockCreated) {
        this.lockCreated = lockCreated;
    }

    public void setLockCreated(Date lockCreatedD) {
        if (lockCreatedD != null) {
            this.lockCreated = new DateState(lockCreatedD);
        } else {
            this.lockCreated = null;
        }
    }

    public DateState getRunDate() {
        return runDate;
    }

    public String getData() {
        return data;
    }

    public String getZooFlagPath() {
        return zooFlagPath;
    }

    public void setZooFlagPath(String zooFlagPath) {
        this.zooFlagPath = zooFlagPath;
    }

    public void setData(String data) {
        this.data = data;
    }

    public String getZooNodeBase() {
        return zooNodeBase;
    }

    public void setZooNodeBase(String zooNodeBase) {
        this.zooNodeBase = zooNodeBase;
    }

    public String getZooProcess() {
        return zooProcess;
    }

    public void setZooProcess(String zooProcess) {
        this.zooProcess = zooProcess;
    }
    
    protected String getIso(DateState dateState)
    {
        if (dateState == null) return null;
        return dateState.getIsoDate();
    }
    
    public String dump(String header)
    {
        String enumS = "";
        String lockCreatedS ="";
        if (lockCreatedS != null) {
            lockCreatedS = " - lockCreated=" + getIso(lockCreated) + "\n";
        }
        String runDateS ="";
        if (runDate != null) {
            runDateS = " - runDate=" + getIso(runDate) + "\n";
        }
        String returnString = "ZooLockState - " + header + "\n"
                + " - zooConnectionString=" + zooConnectionString + "\n"
                + " - zooNodeBase=" + zooConnectionString + "\n" 
                + " - zooProcess=" + zooProcess + "\n"
                + " - zooFlagPath=" + zooFlagPath + "\n"
                + " - tokenStatus=" + tokenStatus + "\n"
                + lockCreatedS
                + runDateS
                + enumS;
        return returnString;
    }
}
