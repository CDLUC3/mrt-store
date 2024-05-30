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
package org.cdlib.mrt.store.action;

import java.util.LinkedHashMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.cdlib.mrt.store.StorageConfig;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.zk.QueueItem;
import org.cdlib.mrt.zk.QueueItemHelper;
import org.cdlib.mrt.zk.Access;
import org.json.JSONObject;
import org.apache.zookeeper.ZooKeeper;
import org.cdlib.mrt.zk.MerrittLocks;
public class AccessLock
{    
    public enum AccessLockType {large, small, invalid};

    public enum AccessLockStatus {on, off, invalid};

    protected static final String NAME = "AccessLock";
    protected static final String MESSAGE = NAME + ": ";
    protected static final boolean DEBUG = false;
    protected static final Logger log4j = LogManager.getLogger();

    protected StorageConfig config = null;
    protected AccessLockType resetType = null;
    protected AccessLockStatus resetStatus = null;
    protected AccessLockStatus oldLargeLockStatus = null;
    protected AccessLockStatus oldSmallLockStatus = null;
    protected AccessLockStatus currentLargeLockStatus = null;
    protected AccessLockStatus currentSmallLockStatus = null;
    protected AccessLockStatus setStatus = null;
    protected String typeS = null;
    protected String operationS = null; 
    protected ZooKeeper zooKeeper = null;
    protected Boolean OK = false;
    protected Boolean initialize = false;
    protected int opNum = 0;
    protected int retry = 0;
    protected Exception retException = null;
    
    public static void main(String[] args) 
            throws Exception
    {
        try {
            
        StorageConfig config = StorageConfig.useYaml();
            setAccessLock(config, "large", "off");
            setAccessLock(config, "small", "off");
            setAccessLock(config, "large", "on");
            setAccessLock(config, "small", "on");
            setAccessLock(config, "large", "off");
            setAccessLock(config, "small", "off");
            
            
            
        } catch (Exception ex) {
            System.out.println("Exception:" + ex);
            ex.printStackTrace();
            
        }
    }
    
    public static JSONObject setAccessLock(StorageConfig config, String typeS, String operationS)
        throws TException
    {
        System.out.println("***setAccess:\n"
                + " - type:" + typeS + "\n"
                + " - operation:" + operationS + "\n"
        );
        AccessLock lock = new AccessLock(config, typeS, operationS);
        lock.process();
        lock.close();
        JSONObject status = lock.getStatus();
        
        System.out.println("STATUS=" + status.toString(2));
            return lock.getStatus();
    }
    
    public AccessLock(
            StorageConfig config,
            String typeS,
            String operationS)
        throws TException
    { 
        this.config = config;
        this.typeS = typeS;
        this.operationS = operationS;
        initialAccessLock();
        log4j.debug("AccessLock:"
                + " - type:" + resetType.toString()
                + " - status:" + resetStatus.toString()
        );
    }
    
    private void initialAccessLock()
        throws TException
    { 
        
        log4j.debug("setAccessLock"
                + " - type:" + typeS
                + " - operation:" + operationS
        );
        
        try {
            String typeC = typeS.toLowerCase();
            resetType = AccessLockType.valueOf(typeC);
            String operationC = operationS.toLowerCase();
            resetStatus = AccessLockStatus.valueOf(operationC);
            log4j.debug("initialAccessLock:" 
                    + " - operationS:" + operationS
                    + " - resetStatus:" + resetStatus.toString()
            );
            zooKeeper = config.getZooKeeper();
            Access.initNodes(zooKeeper);
            log4j.debug("AccessLock - Access initialized");
                        
        } catch (IllegalArgumentException e) {
            
            if (resetType == null) {
                resetType = AccessLockType.invalid;
            }
            if (resetStatus == null) {
                resetStatus = AccessLockStatus.invalid;
            }
         
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
        }
        
        try {
            zooKeeper = config.getZooKeeper();
            Access.initNodes(zooKeeper);
            log4j.debug("AccessLock - Access initialized");
            initialize = true;
                        
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException.GENERAL_EXCEPTION(MESSAGE + "Exception:" + ex);
        }
    }
    
    public void close() 
    {
        try { 
            zooKeeper.close();
        } catch (Exception ex) {
            System.out.println("close Exception:" + ex);
            ex.printStackTrace();
        }
    }
    
    public void process()
        throws TException
    {
        
        oldSmallLockStatus = getSmallLock();
        oldLargeLockStatus = getLargeLock();
                for (retry=0; retry < 4; retry++) {
                    setStatus = setAccessLock(resetType, resetStatus);
                    if (setStatus == resetStatus) {
                        break;
                    }
                    try { Thread.sleep(1000); } catch (Exception e) { }
                }
        OK=true;
        currentSmallLockStatus = getSmallLock();
        currentLargeLockStatus = getLargeLock();
        
        LinkedHashMap<String,String> logHash = new LinkedHashMap<>();
        logHash.put("type", resetType.toString());
        logHash.put("value", resetStatus.toString());
        logHash.put("OK", OK.toString());
        log4j.info("setAccessLock", logHash);
        log4j.debug("process=\n"
                + " - oldSmallLockStatus:" + oldSmallLockStatus + "\n"
                + " - oldLargeLockStatus:" + oldLargeLockStatus + "\n"
                + " - currentSmallLockStatus:" + currentSmallLockStatus + "\n"
                + " - currentLargeLockStatus:" + currentLargeLockStatus + "\n"
        );
    }
    
    public AccessLockStatus setAccessLock(AccessLockType lockType, AccessLockStatus lockStatus)
        throws TException
    {
        AccessLockStatus retStatus = null;
        try {
            if (lockType == AccessLockType.invalid 
                || lockStatus == AccessLockStatus.invalid) {
                opNum=1;
                return AccessLockStatus.invalid;
            }
            
            if (lockType == AccessLockType.small) {
                if (lockStatus == AccessLockStatus.on) {
                    MerrittLocks.lockSmallAccessQueue(zooKeeper);
                    opNum=2;
                    retStatus = getSmallLock();
                    
                } if (lockStatus == AccessLockStatus.off) {
                    MerrittLocks.unlockSmallAccessQueue(zooKeeper);
                    opNum=3;
                    retStatus = getSmallLock();
                }
            } else if (lockType == AccessLockType.large) {
                if (lockStatus == AccessLockStatus.on) {
                    MerrittLocks.lockLargeAccessQueue(zooKeeper);
                    opNum=4;
                    retStatus = getLargeLock();
                    
                } if (lockStatus == AccessLockStatus.off) {
                    MerrittLocks.unlockLargeAccessQueue(zooKeeper);
                opNum=5;
                retStatus = getLargeLock();
                }
            }
            return retStatus;
            
        } catch (Exception ex) {
            System.out.println("setAccessLock Exception:" + ex);
            return AccessLockStatus.invalid;
        }
        
    }

    protected AccessLockStatus getSmallLock()
        throws TException
    {
       
        try {
            if (QueueItemHelper.exists(zooKeeper, QueueItem.ZkPaths.LocksQueueAccessSmall.path)) {
                return AccessLockStatus.on;
            } else  {
                return AccessLockStatus.off;
            }
            
        } catch (Exception ex) {
            System.out.println("getSmallLock Exception:" + ex);
            return AccessLockStatus.invalid;
        }
    }

    protected AccessLockStatus getLargeLock()
        throws TException
    {
       
        try {
            if (QueueItemHelper.exists(zooKeeper, QueueItem.ZkPaths.LocksQueueAccessLarge.path)) {
                return AccessLockStatus.on;
            } else  {
                return AccessLockStatus.off;
            }
            
        } catch (Exception ex) {
            System.out.println("getSmallLock Exception:" + ex);
            return AccessLockStatus.invalid;
        }
    }
    
    public JSONObject getStatus()
    {
        try {
            JSONObject status = new JSONObject();
            status.put("OK", OK);
            status.put("opNum", opNum);
            status.put("accessType", resetType.toString());
            status.put("accessLocked", getCommandLock());
            status.put("initialized", initialize);
            status.put("typeS", typeS);
            status.put("operationS", operationS);
            status.put("resetType", resetType.toString());
            status.put("resetStatus", resetStatus.toString());
            status.put("setStatus", setStatus.toString());
            status.put("retry", retry);
            JSONObject smallLock = new JSONObject();
            smallLock.put("oldStatus", oldSmallLockStatus.toString());
            smallLock.put("currentStatus", currentSmallLockStatus.toString());
            JSONObject largeLock = new JSONObject();
            largeLock.put("oldStatus", oldLargeLockStatus.toString());
            largeLock.put("currentStatus", currentLargeLockStatus.toString());
            status.put("smallLock", smallLock);
            status.put("largeLock", largeLock);
            return status;
            
        } catch (Exception ex) {
            System.out.println("getStatus Exception:" + ex);
            return null;
        }
    }
    
    public Boolean getCommandLock()
    {
        try {
            Boolean currentLock = null;
            boolean smallBool = false;
            boolean largeBool = false;
            if (currentSmallLockStatus == AccessLockStatus.on) smallBool = true;
            if (currentLargeLockStatus == AccessLockStatus.on) largeBool = true;
            if (resetType == AccessLockType.small) return smallBool;
            if (resetType == AccessLockType.large) return largeBool;
            return null;
            
        } catch (Exception ex) {
            System.out.println("getStatus Exception:" + ex);
            return null;
        }
    }
}