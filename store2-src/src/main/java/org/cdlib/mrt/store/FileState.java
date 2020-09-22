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
import java.net.URL;

import org.cdlib.mrt.core.FileComponent;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.store.tools.StoreUtil;
import org.cdlib.mrt.utility.StateInf;

/**
 *
 * @author dloy
 */
public class FileState
        extends FileComponent
        implements StateInf, FileStateInf, Serializable
{
    protected static final String NAME = "FileState";
    protected static final String MESSAGE = NAME + ": ";


    protected URL accessURL = null;
    protected Integer nodeID = null;
    protected Identifier objectID = null;
    protected Integer versionID = null;
    protected URL fileURL = null;

    public FileState(FileComponent fileComponent)
    {
        super(fileComponent);
    }

    public void setAccess(Integer nodeID, URL accessURL, Identifier objectID, Integer versionID)
    {
        this.nodeID = nodeID;
        this.accessURL = accessURL;
        this.objectID = objectID;
        this.versionID = versionID;
    }

    @Override
    public URL getFile() {
        if ((accessURL == null) || (objectID == null)) return null;
        if (fileURL == null) {
            fileURL = StoreUtil.buildContentURL("content", accessURL, nodeID, objectID, versionID, identifier);
        }
        return fileURL;
    }

    public void setFileURL(URL fileURL)
    {
        this.fileURL = fileURL;
    }

    @Override
    public URL getVersionState() {
        if ((accessURL == null) || (objectID == null)) return null;
        return StoreUtil.buildContentURL("state", accessURL, nodeID, objectID, versionID, null);
    }
}

