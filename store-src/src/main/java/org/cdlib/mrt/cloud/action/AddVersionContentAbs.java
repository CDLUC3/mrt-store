/******************************************************************************
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
*******************************************************************************/
package org.cdlib.mrt.cloud.action;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.List;
import java.util.ArrayList;

import org.cdlib.mrt.cloud.VersionMap;

import org.cdlib.mrt.core.DateState;
import org.cdlib.mrt.core.ThreadHandler;

import org.cdlib.mrt.core.ComponentContent;
import org.cdlib.mrt.core.FileComponent;
import org.cdlib.mrt.core.Identifier;


import org.cdlib.mrt.cloud.utility.NormVersionMap;
import org.cdlib.mrt.core.Manifest;
import org.cdlib.mrt.core.ManifestRowAbs;
import org.cdlib.mrt.core.Tika;
import org.cdlib.mrt.store.VersionState;

import org.cdlib.mrt.utility.DateUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.s3.service.CloudResponse;
import org.cdlib.mrt.s3.service.CloudStoreInf;
import org.cdlib.mrt.utility.StringUtil;
/**
 * Run fixity
 * @author dloy
 */
public abstract class AddVersionContentAbs
        extends CloudActionAbs
{

    protected static final String NAME = "AddVersionContentAbs";
    protected static final String MESSAGE = NAME + ": ";
    protected static final boolean DEBUG = false;
    protected static final boolean DEBUGLOW = false;
    protected boolean DEBUGOUT = false;
    protected static final String WRITE = "write";
    protected int threadCnt = 4;

    protected VersionState versionState = null;
    //protected Identifier objectID = null;
    protected boolean validateWrite = false;
    protected NormVersionMap normVersionMap = null;
    protected volatile TException threadTException = null;
    protected long runCnt = 0;
    protected volatile long tallyCnt = 0;
    protected ThreadHandler threads = null;
    protected int nextVersion = -1;
    protected ArrayList<EntryException> failList = new ArrayList<>();
    protected long failItem = -1;
    protected Tika tika = null;
    protected long contentAccumTime = 0;
    protected long writeAccumTime = 0;
    protected long validateAccumTime = 0;
    
    protected AddVersionContentAbs(
            CloudStoreInf s3service,
            String bucket,
            Identifier objectID,
            boolean validateWrite,
            int threadCnt,
            LoggerInf logger)
        throws TException
    {
        super(s3service, bucket, objectID, logger);
        if (objectID == null) {
            throw new TException.INVALID_OR_MISSING_PARM(
                    MESSAGE + "objectID null");
        }
        map =  getVersionMapAdd(bucket, objectID);
        normVersionMap = new NormVersionMap(s3service, logger);
        if (DEBUG) System.out.println(map.dump("AddVersion"));
        this.objectID = objectID;
        this.validateWrite = validateWrite;
        this.threadCnt = threadCnt;
        this.threads = ThreadHandler.getThreadHandler(5, threadCnt, logger);
        tika = new Tika(logger);
        if (DEBUG) {
            System.out.println(MESSAGE + "****start - " + DateUtil.getCurrentIsoDate());
        }
    }
    
    public int writeContent (int nextVersion, ComponentContent versionContent)
        throws TException
    {   
        this.nextVersion = nextVersion;
        try {
            List<FileComponent> inComponents = versionContent.getFileComponents();
            writeComponents(inComponents);
            writeFails();
            finalStatus();
            if (threadTException != null) {
                backOutPut(versionContent);
                throw threadTException;
            }
            if (DEBUG) System.out.println("Final Cnts"
                            + " - runCnt=" + runCnt
                            + " - tallyCnt=" + tallyCnt
                            );
            if (runCnt != tallyCnt) {
                throw new TException.INVALID_ARCHITECTURE(MESSAGE + "Mismatch counts"
                        + " - runCnt=" + runCnt
                        + " - threadCnt=" + tallyCnt
                        );
            }
            setCloudKey(map, objectID, nextVersion, inComponents);
            map.addVersion(inComponents);
            normVersionMap.normalize(map, bucket, objectID);
            if (DEBUG) {
                System.out.println(MESSAGE + "****after - map.addVersion");
            }
            /*
            normVersionMap.normalize(map, bucket, objectID);
            if (DEBUG) {
                System.out.println(MESSAGE + "****after - normVersionMap");
            }
            */
            return map.getCurrent();

        } catch(TException tex) {
            throw tex;
            
        } catch(Exception ex) {
            throw new TException(ex);
        }
    }
    
    public int writeContentUpdate(int nextVersion, InputStream manifestInputStream, String [] deleteList)
        throws TException
    {   
        this.nextVersion = nextVersion;
        List<FileComponent> inComponents = null;
        try {
            if (manifestInputStream != null) {
                Manifest manifest = Manifest.getManifest(logger, ManifestRowAbs.ManifestType.add);
                ComponentContent inVersionContent = new ComponentContent(
                        logger,
                        manifest,
                    manifestInputStream);
                inComponents = inVersionContent.getFileComponents();
                writeComponents(inComponents);
                writeFails();
                finalStatus();
                if (threadTException != null) {
                    backOutPut(inVersionContent);
                    throw threadTException;
                }
                if (DEBUG) System.out.println("Final Cnts"
                                + " - runCnt=" + runCnt
                                + " - tallyCnt=" + tallyCnt
                                );
                if (runCnt != tallyCnt) {
                    throw new TException.INVALID_ARCHITECTURE(MESSAGE + "Mismatch counts"
                            + " - runCnt=" + runCnt
                            + " - threadCnt=" + tallyCnt
                            );
                }
            //setCloudKey(map, objectID, nextVersion, inComponents);
            }
            map.updateVersion(inComponents, deleteList);
            normVersionMap.normalize(map, bucket, objectID);
            if (DEBUG) {
                System.out.println(MESSAGE + "****after - map.updateVersion");
            }
            return map.getCurrent();

        } catch(TException tex) {
            throw tex;
            
        } catch(Exception ex) {
            throw new TException(ex);
        }
    }
    
    public int writeVersionMap()
        throws TException
    {   
        File newManifest = null;
        try {

            newManifest = buildXMLManifest (map, null);
            Exception saveException = null;
            for (int i=0; i < 4; i++) {
                
                try {
                    s3service.deleteManifest(bucket, objectID);
                } catch (Exception ex) { }
  
                try {
                    saveException = null;
                    CloudResponse response = s3service.putManifest(bucket, objectID, newManifest);
                    Exception ex = response.getException();
                    if (ex != null) throw ex;
                    break;
                } catch (Exception ex) {
                    saveException = ex;
                    System.out.println(MESSAGE + " writeVersionMap Exception(" + i + "):" + ex );
                    Thread.sleep(i * 60000);
                }
            }
            if (saveException != null) {
                if (saveException instanceof TException) {
                    throw (TException) saveException;
                } else {
                    throw new TException(saveException);
                }
            }
            
            if (DEBUG) {
                System.out.println(MESSAGE + "****end - " + DateUtil.getCurrentIsoDate());
            }
            return map.getCurrent();

        } catch(TException tex) {
            throw tex;
            
        } catch(Exception ex) {
            throw new TException(ex);
            
        } finally {
            if (newManifest != null) {
                try {
                    newManifest.delete();
                } catch (Exception ex) { }
            }
        }
    }
    
    
    public int writeComponents (List<FileComponent> inComponents)
        throws TException
    {   
        try {
            int addCnt = 0;
            for (FileComponent component: inComponents) {
                if (DEBUGLOW) System.out.println("---(" + System.nanoTime() + ") Add component:" + component.getIdentifier());
                if (threadTException != null) {
                    break;
                }
                if (component.getCreated() == null) {
                    if (DEBUGLOW) System.out.println("---(" + System.nanoTime() + ") set created:" + component.getIdentifier());
                    component.setCreated(new DateState());
                }
                if (DEBUGLOW) System.out.println("---(" + System.nanoTime() + ") map.setCloudComponent:" + component.getIdentifier());
                map.setCloudComponent(component, false);
                if (DEBUG) System.out.println("writeContent:"
                        + " - bucket:" + bucket
                        + " - objectID:" + objectID.getValue()
                        + " - versionID:" + nextVersion
                        + " - fileID:" + component.getIdentifier()
                        );
                if (DEBUG) System.out.println(component.dump("writeContent"));
                String localKey = component.getLocalID();
                if (localKey != null) {
                    org.cdlib.mrt.s3.service.CloudUtil.KeyElements parts = org.cdlib.mrt.s3.service.CloudUtil.getKeyElements(localKey);
                    int keyVersion = parts.versionID;
                    if (keyVersion == nextVersion) {
                        component.setLocalID(null);
                    }
                }
                if (component.getLocalID() == null) {
                    if (DEBUGOUT) System.out.println(component.dump("upload component"));
                    if (DEBUGLOW) System.out.println("---(" + System.nanoTime() + ") AddComponent:" + component.getIdentifier());
                    AddComponent addComponent = new AddComponent(component, nextVersion, addCnt);
                    if (DEBUGLOW) System.out.println("---(" + System.nanoTime() + ") threads.runThread:" + component.getIdentifier());
                    threads.runThread(addComponent);
                    if (DEBUGLOW) System.out.println("---(" + System.nanoTime() + ") after threads.runThread:" + component.getIdentifier());
                    runCnt++;
                    addCnt++;
                    //if ((runCnt % 100) == 0) {
                    if (DEBUGOUT) System.out.println("Cnts"
                            + " - runCnt=" + runCnt
                            + " - threadCnt=" + tallyCnt
                            + " - failSize=" + failList.size()
                            );
                    //}
                }
            }
            return addCnt;

        } catch(Exception ex) {
            setException(ex);
            return 0;
        }
    }
    
    public int writeFails()
        throws TException
    {   
        int failTries = 0;
        ArrayList<FileComponent> processList = new ArrayList<>();
        try {
            while(true) {
                processList.clear();
                threads.shutdown();
                if (failList.size() == 0) return 0;
                if (failTries >= 1) return failTries;
                threads.setShutdown(false); //!!!!!!!!
                if (true) System.out.println("***writeFail(" + failTries + "):" 
                        + " - failList.size=" + failList.size()
                        + " - processList.size=" + processList.size()
                        );
                long sleeptime = failTries * 5000;
                try {
                    Thread.sleep(sleeptime);
                } catch (Exception ex) { }
                failTries++;
                moveFailToProcess(processList);
                writeComponents (processList);
            }

        } catch(TException tex) {
            threadTException = tex;
            return 0;
        }
    }
    
    protected void finalStatus()
        throws TException
    {
        if (failList.size() == 0) {
            logger.logMessage("Object OK" 
                + " - objectID=" + objectID.getValue()
                + " - version=" + nextVersion
                ,0,true);
            return;
        }
        logger.logMessage("Object Fails" 
            + " - objectID=" + objectID.getValue()
            + " - version=" + nextVersion
            ,0,true);
        for (EntryException entry : failList) {
            FileComponent component = entry.fileComponent;
            logger.logMessage("Object Fails component" 
                + " - objectID=" + objectID.getValue()
                + " - version=" + nextVersion
                + " - file=" + component.getIdentifier()
                + " - Exception:" + entry.componentException
                ,0,true);
        }
        threadTException =  new TException.GENERAL_EXCEPTION("Object Fails:"
                + " - objectID=" + objectID.getValue()
                + " - version=" + nextVersion
                );
    }
    
    protected void moveFailToProcess(List<FileComponent> processList)
    {
        for (EntryException entry : failList) {
            FileComponent component = entry.fileComponent;
            processList.add(component);
        }
        failList.clear();
    }
    
    
    protected void setCloudKey(
                VersionMap versionMap,
                Identifier objectID,
                int versionID,
                List<FileComponent> components)
        throws TException
    {
        try {
            for (FileComponent component : components) {
                String key = org.cdlib.mrt.s3.service.CloudUtil.getKey(objectID, versionID, component.getIdentifier(), false);
                component.setLocalID(key);
                versionMap.setCloudComponent(component, false);
            }
            
        } catch (Exception ex) {
            setException(ex);
        }
        
    }
    
    public void backOutPut(ComponentContent versionContent)
        throws TException
    {   
        try {
            List<FileComponent> inComponents = versionContent.getFileComponents();
            for (FileComponent component: inComponents) {
                if ((component.getTitle() != null) && component.getTitle().equals(WRITE)) {
                    String key = component.getLocalID();
                    CloudResponse delResponse = s3service.deleteObject(bucket, key);
                    String status = "OK";
                    if (delResponse.getException() != null) status="FAIL";
                    log("backOutException DELETE key=" + key + " - status=" + status);
                }
            }

        } catch(Exception ex) {
            setException(ex);
        }
    }    
    
    public synchronized void  bumpTallyCnt() {
        tallyCnt++;
    }

    public synchronized void  setThreadTException(TException threadTException) {
        this.threadTException = threadTException;
    }

    public synchronized void  setThreadFail(FileComponent fileComponent, TException tex) {
        File dFile = fileComponent.getComponentFile();
        try {
            if (dFile != null) {
                dFile.delete();
            }
        } catch (Exception ex) { };
        fileComponent.setComponentFile(null);
        fileComponent.setLocalID(null);
        runCnt--;
        EntryException failEntry = new EntryException(fileComponent, tex);
        failList.add(failEntry);
        String msg  = "***FAIL add list"
                + " - objectID=" + objectID.getValue()
                + " - version=" + nextVersion
                + " - file=" + fileComponent.getIdentifier()
                + " - Exception=" + tex;
        System.out.println(msg);
        logger.logMessage(msg, 1, true);
        if (this.threadTException != null)this.threadTException.printStackTrace();
    }

    public synchronized void  setThreadStop(TException tex, FileComponent fileComponent) {
        File dFile = fileComponent.getComponentFile();
        try {
            if (dFile != null) {
                dFile.delete();
            }
        } catch (Exception ex) { };
        threadTException = tex;
        String msg  = "***FAIL STOP"
                + " - objectID=" + objectID.getValue()
                + " - version=" + nextVersion
                + " - file=" + fileComponent.getIdentifier()
                + " - Exception=" + this.threadTException;
        System.out.println(msg);
        logger.logMessage(msg, 1, true);
        threadTException.printStackTrace();
    }

    public void setValidateWrite(boolean validateWrite) {
        this.validateWrite = validateWrite;
    }
    
    public void setThreadCnt(int threadCnt)
    {
        this.threadCnt = threadCnt;
    }
    
    public abstract void setFile(FileComponent manifestComponent)
        throws TException;

    public void setFailItem(long failItem) {
        this.failItem = failItem;
    }

    public void setDEBUGOUT(boolean DEBUGOUT) {
        this.DEBUGOUT = DEBUGOUT;
    }
    
    public class AddComponent
        implements Runnable
    {    
        protected FileComponent component = null;
        protected int nextVersion = 0;
        protected long cnt = -1;
        
        public AddComponent(
            FileComponent component,
            int nextVersion,
            long cnt) 
        {
            this.component = component;
            this.nextVersion = nextVersion;
            this.cnt = cnt;
        }
        
        public void run()
        {
            if (DEBUGLOW) System.out.println("***(" + System.nanoTime() + ") Thread:" + objectID.getValue());
            writeComponent();
        }
        
        public void writeComponent ()
        {   
            File componentFile = null;
            try {
                if (DEBUGLOW) System.out.println("***(" + System.nanoTime() + ") Fill:" + component.getIdentifier());
                long componentBeforeTime = DateUtil.getEpochUTCDate();
                fillComponent(component);
                componentFile = component.getComponentFile();
                
                long componentAfterTime = DateUtil.getEpochUTCDate();
                contentAccumTime += (componentAfterTime - componentBeforeTime);
                if (DEBUGLOW) System.out.println("***(" + System.nanoTime() + ") Start:" + component.getIdentifier());
                CloudResponse response = s3service.putObject(
                        bucket, 
                        objectID, 
                        nextVersion, 
                        component.getIdentifier(), 
                        component.getComponentFile());
                
                long writeAfterTime = DateUtil.getEpochUTCDate();
                writeAccumTime += (writeAfterTime - componentAfterTime);
                if (DEBUGLOW) System.out.println("***(" + System.nanoTime() + ") After s3service.putObject:" + component.getIdentifier());
                Exception putException = response.getException();
                //!!!
                //System.out.println(">>>fail - item=" + failItem + " - cnt=" + cnt);
                if ((failItem >= 0) && (failItem == cnt)) {
                    System.out.println("***TEST EXCEPTION="
                            + component.dump("TESTEXCEPTION")
                        );
                    putException = new TException.GENERAL_EXCEPTION("test");
                }
                if (putException != null) {
                    throw putException;
                }

                String retKey = response.getStorageKey();
                component.setLocalID(retKey);
                component.setTitle(WRITE);

                if (validateWrite) {
                    validateComponent(nextVersion, component);
                } else {
                    String msg = ""
                        + " - objectID=" + objectID.getValue()
                        + " - versionID=" + nextVersion
                        + " - fileID=" + component.getIdentifier()
                        + " - key=" + component.getLocalID()
                        + " - validateWrite=" + validateWrite;
                    logger.logMessage("Component fixity not performed:" 
                            + msg, 
                            0, true);
                }
                long writeValidateTime = DateUtil.getEpochUTCDate();
                validateAccumTime += (writeValidateTime - writeAfterTime);
                bumpTallyCnt();
                if (DEBUGLOW) System.out.println("***(" + System.nanoTime() + ") End:" + component.getIdentifier());
                
            } catch(TException tex) {
                System.out.println("**--**writeComponent TException:" + tex);
                tex.printStackTrace();
                setThreadFail(component, tex);
                
            } catch(Exception ex) {
                System.out.println("writeComponent Exception:" + ex);
                TException tex = new TException(ex);
                setThreadFail(component, tex);
                
            } finally {
                try {
                    if (componentFile != null) {
                        componentFile.delete();
                        if (componentFile.exists()) {
                            System.out.println("***Component delete failure:" 
                                    + componentFile.getAbsolutePath());
                        } else {
                            if (DEBUG) System.out.println("+++Component deleted:" 
                                    + componentFile.getAbsolutePath());
                        }
                    }
                } catch (Exception ex) { 
                    System.out.println("***Component delete exception(" 
                                    + componentFile.getAbsolutePath() + "):" + ex);
                }
            }
        } 
    
        public void fillComponent(FileComponent manifestComponent)
            throws TException
        {
            
            try {
                setFile(manifestComponent);
                File tmpFile = manifestComponent.getComponentFile();
                
                String mimeType = manifestComponent.getMimeType();
                if (StringUtil.isNotEmpty(mimeType)) {
                    return;
                }
                InputStream componentStream = null;
                try {
                    componentStream = new FileInputStream(tmpFile);
                    mimeType = tika.getMimeType(componentStream, manifestComponent.getIdentifier());
                    component.setMimeType(mimeType);
                    if (DEBUG) System.out.println("add mimeType=" + mimeType);
                    
                }  catch (Exception ex) {
                    System.out.println("WARNING tika exception:" + ex);
                    
                } finally {
                    try {
                        if (componentStream != null) {
                            componentStream.close();
                        }
                    } catch (Exception ex) { }
                }


            } catch (TException tex) {
                throw tex;

            } catch (Exception ex) {
                String msg = MESSAGE + "buildCloudComponent - Exception:" + ex;
                logger.logError(msg, 2);
                logger.logError(StringUtil.stackTrace(ex), 10);
                throw new TException.GENERAL_EXCEPTION(msg);
            }

        }
    }
    
    private static class EntryException
    {
        public final FileComponent fileComponent;
        public final TException  componentException;
        public EntryException(FileComponent fileComponent, TException  componentException)
        {
            this.fileComponent = fileComponent;
            this.componentException = componentException;
        }
    }
}

