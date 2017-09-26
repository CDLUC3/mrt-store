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

import org.cdlib.mrt.store.ContextLocalID;
import java.io.Serializable;
import java.util.Date;
import org.cdlib.mrt.core.DateState;
import org.cdlib.mrt.store.je.LocalIDMap;
import org.cdlib.mrt.utility.StateInf;

/**
 *
 * @author dloy
 */
public class PrimaryIDState
        implements StateInf, PrimaryIDStateInf, Serializable
{
    protected static final String NAME = "PrimaryIDState";
    protected static final String MESSAGE = NAME + ": ";

    protected String localContext = null;
    protected String localIdentifier = null;
    protected boolean exists = false;
    protected String primaryIdentifier = null;
    protected DateState  timestamp = null;
    protected Integer physicalNode = null;

    public PrimaryIDState(LocalIDMap map)
    {
        setLocalContext(map.getContext());
        setLocalIdentifier(map.getLocalID());
        setPrimaryIdentifier(map.getPrimaryID());
        setTimestamp(map.getTimestamp());
    }

    public PrimaryIDState(String context, String localID)
    {
        localContext = context;
        localIdentifier = localID;
    }

    public PrimaryIDState(String primaryID)
    {
        primaryIdentifier = primaryID;
    }

    @Override
    public boolean isExists() {
        return exists;
    }

    public void setExists(boolean exists) {
        this.exists = exists;
    }

    @Override
    public String getLocalContext() {
        return localContext;
    }

    public void setLocalContext(String localContext) {
        this.localContext = localContext;
    }

    @Override
    public String getLocalIdentifier() {
        return localIdentifier;
    }

    public void setLocalIdentifier(String localIdentifier) {
        this.localIdentifier = localIdentifier;
    }

    @Override
    public String getPrimaryIdentifier() {
        return primaryIdentifier;
    }

    public void setPrimaryIdentifier(String primaryIdentifier) {
        this.primaryIdentifier = primaryIdentifier;
        this.exists = true;
    }

    @Override
    public DateState getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = new DateState(timestamp);
    }
    
    public ContextLocalID retrieveContextLocalID()
    {
        return new ContextLocalID(
                primaryIdentifier,
                null,
                localContext,
                localIdentifier,
                timestamp
                );
    }

    @Override
    public Integer getPhysicalNode() {
        return physicalNode;
    }

    public void setPhysicalNode(Integer physicalNode) {
        this.physicalNode = physicalNode;
    }

    @Override
    public String dump(String header)
    {
        return header
                + " - localContext=" + localContext
                + " - localIdentifier=" + localIdentifier
                + " - exists=" + exists
                + " - primaryIdentifier=" + primaryIdentifier
                + " - timestamp=" + timestamp
                ;
    }

}

