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

import java.util.ArrayList;
import java.util.Date;
import java.io.Serializable;
import java.net.URL;

import org.cdlib.mrt.utility.StateInf;
import org.cdlib.mrt.core.DateState;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.core.MessageDigest;


import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.core.MessageDigest;
import org.cdlib.mrt.store.tools.StoreUtil;
import org.cdlib.mrt.utility.StateInf;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.DateUtil;
import org.cdlib.mrt.utility.StringUtil;

/**
 *
 * @author dloy
 */
public class ObjectFixityState
        implements Serializable, StateInf
{
    protected int physicalNode = 0;
    protected Identifier objectID = null;
    protected int versionCnt = 0;
    protected int fileCnt = 0;
    protected int errorCnt = 0;
    protected int exCnt = 0;
    protected boolean match = false;
    protected ArrayList<FileFixityState> listErrors= new ArrayList<>();

    public ObjectFixityState() { }
    public Identifier getObjectID() {
        return objectID;
    }

    public ObjectFixityState setObjectID(Identifier objectID) {
        this.objectID = objectID;
        return this;
    }

    public int getVersionCnt() {
        return versionCnt;
    }

    public ObjectFixityState setVersionCnt(int versionCnt) {
        this.versionCnt = versionCnt;
        return this;
    }

    public int getFileCnt() {
        return fileCnt;
    }

    public ObjectFixityState setFileCnt(int fileCnt) {
        this.fileCnt = fileCnt;
        return this;
    }

    public int getPhysicalNode() {
        return physicalNode;
    }

    public ObjectFixityState setPhysicalNode(int physicalNode) {
        this.physicalNode = physicalNode;
        return this;
    }

    public boolean isMatch() {
        return match;
    }

    public ObjectFixityState setMatch(boolean match) {
        this.match = match;
        return this;
    }

    public ArrayList<FileFixityState> getListErrors() {
        return listErrors;
    }

    public ObjectFixityState setListErrors(ArrayList<FileFixityState> listErrors) {
        this.listErrors = listErrors;
        return this;
    }

    public int getErrorCnt() {
        return errorCnt;
    }

    public ObjectFixityState setErrorCnt(int errorCnt) {
        this.errorCnt = errorCnt;
        return this;
    }
    
    public void add(FileFixityState fileFixityState) {
       listErrors.add(fileFixityState);
    }

    public int getExCnt() {
        return exCnt;
    }

    public ObjectFixityState setExCnt(int exCnt) {
        this.exCnt = exCnt;
        return this;
    }
}
