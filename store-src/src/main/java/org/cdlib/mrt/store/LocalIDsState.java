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

import java.io.Serializable;
import java.util.Vector;
import org.cdlib.mrt.core.DateState;
import org.cdlib.mrt.store.je.LocalIDMap;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.StateInf;

/**
 *
 * @author dloy
 */
public class LocalIDsState
        implements StateInf, Serializable
{
    protected static final String NAME = "LocalIDsState";
    protected static final String MESSAGE = NAME + ": ";

    protected String primaryIdentifier = null;
    protected DateState  timestamp = null;
    protected Vector<ContextLocalID> list = new Vector<ContextLocalID>();
    protected Integer physicalNode = null;

    public LocalIDsState() { }

    public void addLocalID(LocalIDMap map)
    {
        if (StringUtil.isEmpty(primaryIdentifier)) setPrimaryIdentifier(map.getPrimaryID());
        addMap(map);
    }

    public boolean isExists() {
        if (list.size() > 0) return true;
        return false;
    }

    public String getPrimaryIdentifier() {
        return primaryIdentifier;
    }

    public void setPrimaryIdentifier(String primaryIdentifier) {
        this.primaryIdentifier = primaryIdentifier;
    }

    public Vector<ContextLocalID> getLocalIDs()
    {
        if (list.size() == 0) return null;
        return list;
    }

    public int getCountLocalIDs()
    {
        return list.size();
    }
    
    protected void addMap(LocalIDMap map)
    {
        ContextLocalID localID = new ContextLocalID(map);
        list.add(localID);
    }

    public Integer getPhysicalNode() {
        return physicalNode;
    }

    public void setPhysicalNode(Integer physicalNode) {
        this.physicalNode = physicalNode;
    }

    public String dump(String header)
    {
        StringBuffer buf = new StringBuffer();
        buf.append(header);
        int i=0;
        for (ContextLocalID id: list) {
            i++;
            buf.append(id.dump("***>(" + i + ")<***"));
        }
        return buf.toString();
    }
}

