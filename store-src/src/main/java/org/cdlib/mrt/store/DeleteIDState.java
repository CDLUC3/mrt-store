/*
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
**********************************************************/
package org.cdlib.mrt.store;

import java.io.Serializable;
import java.util.Vector;
import org.cdlib.mrt.core.DateState;
import org.cdlib.mrt.store.je.LocalIDMap;
import org.cdlib.mrt.utility.StateInf;

/**
 *
 * @author dloy
 */
public class DeleteIDState
        implements StateInf, DeleteIDStateInf, Serializable
{
    private static final long serialVersionUID = 31L;
    protected static final String NAME = "DeleteIDState";
    protected static final String MESSAGE = NAME + ": ";

    protected String requestContext = null;
    protected String requestLocalID = null;
    protected String requestPrimaryID = null;
    protected Vector<ContextLocalID> deletedLocalIDs = new Vector<ContextLocalID>();
    protected String deletedPrimaryID = null;
    protected boolean exists = false;
    protected boolean deleted = false;
    protected DateState  timestamp = null;

    public DeleteIDState(LocalIDMap map)
    {
        requestContext = map.getContext();
        requestLocalID = map.getLocalID();
        requestPrimaryID = map.getPrimaryID();
    }

    public DeleteIDState(String context, String localID)
    {
        requestContext = context;
        requestLocalID = localID;
    }

    public DeleteIDState(String primaryID)
    {
        requestPrimaryID = primaryID;
    }

    public DeleteIDState(String context, String localID, String primaryID)
    {
        requestContext = context;
        requestLocalID = localID;
        requestPrimaryID = primaryID;
    }


    public void addContextLocalID(ContextLocalID contextLocalID)
    {
        if (contextLocalID == null) return;
        deletedLocalIDs.add(contextLocalID);
    }

    @Override
    public Vector<ContextLocalID> getDeletedLocalIDs() {
        return deletedLocalIDs;
    }

    public void setDeletedLocalIDs(Vector<ContextLocalID> deletedLocalIDs) {
        this.deletedLocalIDs = deletedLocalIDs;
    }


    @Override
    public String getDeletedPrimaryID() {
        return deletedPrimaryID;
    }

    public void setDeletedPrimaryID(String deletedPrimaryID) {
        this.deletedPrimaryID = deletedPrimaryID;
    }

    @Override
    public boolean isDeleted() {
        return deleted;
    }

    public void setDeleted(boolean deleted) {
        this.deleted = deleted;
    }

    @Override
    public boolean isExists() {
        return exists;
    }

    public void setExists(boolean exists) {
        this.exists = exists;
    }

    @Override
    public String getRequestContext() {
        return requestContext;
    }

    public void setRequestContext(String requestContext) {
        this.requestContext = requestContext;
    }

    @Override
    public String getRequestLocalID() {
        return requestLocalID;
    }

    public void setRequestLocalID(String requestLocalID) {
        this.requestLocalID = requestLocalID;
    }

    @Override
    public String getRequestPrimaryID() {
        return requestPrimaryID;
    }

    public void setRequestPrimaryID(String requestPrimaryID) {
        this.requestPrimaryID = requestPrimaryID;
    }

    @Override
    public DateState getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(DateState timestamp) {
        this.timestamp = timestamp;
    }


}

