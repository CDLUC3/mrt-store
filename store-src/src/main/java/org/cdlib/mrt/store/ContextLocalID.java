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
import java.util.Date;
import org.cdlib.mrt.core.DateState;
import org.cdlib.mrt.store.je.LocalIDMap;
import org.cdlib.mrt.utility.StateInf;

/**
 *
 * @author dloy
 */
public class ContextLocalID
        implements StateInf, Serializable
{
    protected static final String NAME = "ContextLocalID";
    protected static final String MESSAGE = NAME + ": ";

    protected String primaryID = null;
    private String contextLocalID = null;
    protected String context = null;
    protected String localID = null;
    protected DateState  timestamp = null;

    public ContextLocalID(LocalIDMap map) {
        this.contextLocalID = map.getContextLocalID();
        this.context = map.getContext();
        this.localID = map.getLocalID();
        this.primaryID = map.getPrimaryID();
        setTimestamp(map.getTimestamp());
    }

    public ContextLocalID(
            String primaryID,
            String contextLocalID,
            String context,
            String localID,
            DateState  timestamp)
    {
        this.contextLocalID = contextLocalID;
        this.context = context;
        this.localID = localID;
        this.primaryID = primaryID;
        this.timestamp = timestamp;
    }

    public String getContext() {
        return context;
    }

    public void setContext(String context) {
        this.context = context;
    }

    public String getLocalID() {
        return localID;
    }

    public void setLocalID(String localID) {
        this.localID = localID;
    }

    public DateState getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = new DateState(timestamp);
    }

    public String retrievePrimaryIdentifier()
    {
        return primaryID;
    }

    public void setPrimaryIdentifier(String primaryIdentifier) {
        this.primaryID = primaryIdentifier;
    }

    public String retrieveContextLocalID() {
        return contextLocalID;
    }

    public void setContextLocalID(String contextLocalID) {
        this.contextLocalID = contextLocalID;
    }


    public String dump(String header)
    {
        return header
                + " - contextLocalID=" + contextLocalID
                + " - context=" + context
                + " - localID=" + localID
                + " - primaryID=" + primaryID
                + " - timestamp=" + timestamp
                ;
    }
}