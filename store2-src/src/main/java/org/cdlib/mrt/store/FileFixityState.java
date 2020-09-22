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
public class FileFixityState
        implements FileFixityStateInf, Serializable, StateInf
{
    protected Identifier objectID = null;
    protected int versionID = 0;
    protected String fileName = null;
    protected boolean sizeMatches = false;
    protected boolean digestMatches = false;
    protected DateState fixityDate = null;
    protected long manifestFileSize;
    protected long fileSize;
    protected MessageDigest fileDigest = null;
    protected MessageDigest manifestDigest = null;
    protected Integer physicalNode = null;
    protected String key = null;
    protected Exception ex = null;

    protected URL accessURL = null;
    protected Integer nodeID = null;

    public void setAccess(Integer nodeID, URL accessURL)
    {
        this.nodeID = nodeID;
        this.accessURL = accessURL;
    }

    public String dump(String header) {
        return "FileFixityState:" + header
                + " - sizeMatches:" + sizeMatches
                + " - digestMatches:" + digestMatches
                + " - objectID:" + objectID
                + " - versionID:" + versionID
                + " - fileName:" + fileName
                + " - manifestFileSize:" + manifestFileSize
                + " - fileSize:" + fileSize
                + " - fileDigest:" + fileDigest
                + " - manifestDigest:" + manifestDigest;
    }

    @Override
    public URL getFile() {
        if ((accessURL == null) || (objectID == null)) return null;
        return StoreUtil.buildContentURL("fixity", accessURL, nodeID, objectID, versionID, fileName);
    }

    public Identifier getObjectID() {
        return objectID;
    }

    public void setObjectID(Identifier objectID) {
        this.objectID = objectID;
    }

    public int getVersionID() {
        return versionID;
    }

    public void setVersionID(int versionID) {
        this.versionID = versionID;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public boolean getSizeMatches() {
        return sizeMatches;
    }

    public void setSizeMatches(boolean sizeMatches) {
        this.sizeMatches = sizeMatches;
    }

    public boolean getDigestMatches() {
        return digestMatches;
    }

    public void setDigestMatches(boolean digestMatches) {
        this.digestMatches = digestMatches;
    }

    public DateState getFixityDate() {
        return fixityDate;
    }

    public void setFixityDate(DateState fixityDate) {
        this.fixityDate = fixityDate;
    }

    public void setFixityDate(Date date) {
        if (date != null) {
            fixityDate = new DateState(date);
        }
    }


    public long getFileSize() {
        return fileSize;
    }

    public void setFileSize(long fileSize) {
        this.fileSize = fileSize;
    }

    public MessageDigest getManifestDigest() {
        return manifestDigest;
    }

    public void setManifestDigest(MessageDigest manifestDigest) {
        this.manifestDigest = manifestDigest;
    }

    public MessageDigest getFileDigest() {
        return fileDigest;
    }

    public void setFileDigest(MessageDigest fileDigest) {
        this.fileDigest = fileDigest;
    }

    public long getManifestFileSize() {
        return manifestFileSize;
    }

    public void setManifestFileSize(Long manifestFileSize) {
        this.manifestFileSize = manifestFileSize;
    }

    public Integer getPhysicalNode() {
        return physicalNode;
    }

    public void setPhysicalNode(Integer physicalNode) {
        this.physicalNode = physicalNode;
    }

    public Exception getEx() {
        return ex;
    }

    public void setEx(Exception ex) {
        this.ex = ex;
    }

    public boolean isSizeMatches() {
        return sizeMatches;
    }

    public boolean isDigestMatches() {
        return digestMatches;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }
    
}
