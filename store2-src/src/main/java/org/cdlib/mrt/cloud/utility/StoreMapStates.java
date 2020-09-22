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
package org.cdlib.mrt.cloud.utility;




import org.cdlib.mrt.cloud.ManInfo;
import org.cdlib.mrt.cloud.VersionMap;
import org.cdlib.mrt.core.ComponentContent;
import org.cdlib.mrt.core.FileComponent;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.store.FileState;
import org.cdlib.mrt.store.ObjectState;
import org.cdlib.mrt.store.VersionState;
import org.cdlib.mrt.utility.TException;

/**
 * This object imports the formatTypes.xml and builds a local table of supported format types.
 * Note, that the ObjectFormat is being deprecated and replaced by a single format id (fmtid).
 * This change is happening because formatName is strictly a description and has no functional
 * use. The scienceMetadata flag is being dropped because the ORE Resource Map is more flexible
 * and allows for a broader set of data type.
 * 
 * @author dloy
 */
public class StoreMapStates
{
    private static final String NAME = "VersionMap";
    private static final String MESSAGE = NAME + ": ";

    private static final String NL = System.getProperty("line.separator");
    private static final boolean DEBUG = false;

    public static FileState getFileState(VersionMap map, int versionID, String fileID)
        throws TException
    {
        FileComponent component = map.getFileComponent(versionID, fileID);
        if (component == null) {
            throw new TException.REQUESTED_ITEM_NOT_FOUND("component not found:" + fileID);
        }
        
        FileState fileState = new FileState(component);
        fileState.setLocalID(component.getLocalID());
        return fileState;
        
    }
    
    public static VersionState getVersionState(VersionMap map, int versionID)
        throws TException
    {
        ComponentContent content = map.getVersionContent(versionID);
        if (content == null) {
            throw new TException.REQUESTED_ITEM_NOT_FOUND("versionID not found:" + versionID);
        }
        VersionMap.VersionStats stats = map.getVersionStats(versionID);
        ManInfo info = map.getVersionInfo(versionID);
        VersionMap.DeltaStats delta = map.getDeltaStats();
        Identifier objectID = map.getObjectID();
        int current = map.getCurrent();
        
        VersionState versionState = new VersionState();
        versionState.setObjectID(objectID);
        versionState.setIdentifier(versionID);
        versionState.setNumActualFiles(stats.numActualFiles);
        versionState.setTotalActualSize(stats.totalActualSize);
        versionState.setNumFiles(stats.numFiles);
        versionState.setTotalSize(stats.totalSize);
        versionState.setCurrent(versionID == current);
        versionState.setCreated(info.created);
        if (versionID == current) {
            versionState.setDeltaNumFiles(delta.deltaActualCount);
            versionState.setDeltaSize(delta.deltaActualSize);
        }
        versionState.setFileNames(content);
        return versionState;
        
    }
    
    public static ObjectState getObjectState(VersionMap map)
        throws TException
    {
        Identifier objectID = map.getObjectID();
        ObjectState objectState = ObjectState.getObjectState(objectID);
        for (int i=1; i <= map.getVersionCount(); i++) {
            VersionState versionState = getVersionState(map, i);
            objectState.addVersion(versionState);
        }
        objectState.setNumVersions(map.getVersionCount());
        objectState.setLastAddVersion(map.getLastAddVersion());
        objectState.setLastDeleteVersion(map.getLastDeleteVersion());
        objectState.setNumFiles(map.getTotalCnt());
        objectState.setSize(map.getTotalSize());
        objectState.setNumActualFiles(new Long(map.getActualCnt()));
        objectState.setTotalActualSize(map.getActualSize());
        
        return objectState;
        
    }
}
