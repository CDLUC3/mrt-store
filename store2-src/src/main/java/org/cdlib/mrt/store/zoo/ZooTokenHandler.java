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
package org.cdlib.mrt.store.zoo;

/**
 *
 * @author replic
 */
import java.util.Date;
import java.io.Serializable;
import java.net.URL;

import org.cdlib.mrt.utility.StateInf;
import org.cdlib.mrt.core.DateState;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.core.MessageDigest;


import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.core.MessageDigest;
import org.cdlib.mrt.queue.LockItem;
import org.cdlib.mrt.store.tools.StoreUtil;
import org.cdlib.mrt.utility.StateInf;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.DateUtil;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StringUtil;
public class ZooTokenHandler {
    protected static final String NAME = "ZooTokenManager";
    protected static final String MESSAGE = NAME + ": ";
    protected static final Boolean DEBUG = false;
    
    protected String zooConnectionString = null;
    protected String zooNodeBase = null;
    protected ZooFlagsEnum lock = null;
    
    //protected ZooTokenManager zooTokenManager = null;
    protected LoggerInf logger = null;
            
    public static ZooTokenHandler getZooTokenHandler (
            String zooConnectionString, 
            String zooNodeBase,
            ZooFlagsEnum lock,
            LoggerInf logger)
        throws TException
    {
        if (DEBUG) System.out.println(MESSAGE
                + " - zooConnectionString:" + zooConnectionString
                + " - zooNodeBase:" + zooNodeBase
                + " - lock.getHttpName:" + lock.getHttpName()
                + " - lock.getZooPath:" + lock.getZooPath()
        );
        if (StringUtil.isAllBlank(zooConnectionString)) {
            throw new TException.INVALID_OR_MISSING_PARM("zooConnectionString not found");
        }
        if (StringUtil.isAllBlank(zooNodeBase)) {
            throw new TException.INVALID_OR_MISSING_PARM("zooLockNode not found");
        }
        if (lock == null) {
            throw new TException.INVALID_OR_MISSING_PARM("ZooFlagsEnum not found");
        }
        return new ZooTokenHandler(zooConnectionString, zooNodeBase, lock, logger);
    }
    
    protected ZooTokenHandler(
            String zooConnectionString, 
            String zooNodeBase,
            ZooFlagsEnum lock,
            LoggerInf logger)
        throws TException
    {
        this.zooConnectionString = zooConnectionString;
        this.zooNodeBase = zooNodeBase;
        this.logger = logger;
        this.lock = lock;
        System.out.println(MESSAGE
                + " - zooConnectionString:" + zooConnectionString
                + " - zooNodeBase:" + zooNodeBase
                + " - lock.getHttpName:" + lock.getHttpName()
                + " - lock.getZooPath:" + lock.getZooPath()
        );
        //this.zooTokenManager = ZooTokenManager.getZooTokenManager(zooConnectString, zooNodeBase, logger);
    }
    
    public static Boolean statusFlag(String valueS)
    {
        if (valueS == null) return null;
        if (valueS.length() == 0) return null;
        valueS = valueS.toLowerCase();
        if (valueS.equals("off")) return false;
        if (valueS.equals("on")) return true;
        return null;
    }
    
    public ZooTokenState processFlag(String operation)
        throws TException
    {
        if (StringUtil.isAllBlank(operation)) {
            throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + " set operation missing");
        }
        Boolean opStat = null;
        LockItem item = null;
        try {
           
            operation = operation.toLowerCase();
            if (DEBUG) System.out.println("Operation:" + operation);
            if (operation.equals("seton")) {
                opStat = set();

            } else if (operation.equals("setoff")) {
                opStat = remove();

            } else if (operation.equals("status")) {
                opStat = verify();
                if (DEBUG) System.out.println("opStat:" + opStat);

            } else if (operation.equals("state")) {
                item = getFlagState();
                if (item != null) opStat = true;
                else {
                    if (DEBUG) System.out.println("opStat=null");
                    opStat = false;
                }
            } else {
                throw new TException.INVALID_OR_MISSING_PARM(MESSAGE + " set operation not found:" + operation);
            }
            ZooTokenState state = getState(operation, opStat, item);
            return state;
            
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
            
        }
    }
    
    public Boolean set()
        throws TException
    {
        if (lock == null) {
            throw new TException.INVALID_OR_MISSING_PARM("ZooFlagsEnum not found");
        }
        ZooTokenManager manager = null;
        try {
            String lockPath = lock.getZooPath();
            manager = ZooTokenManager.getZooTokenManager(zooConnectionString, zooNodeBase, logger);
            String payload = "set " + lockPath;
            Boolean set = manager.addLock(lockPath, payload);
            return set;
            
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
            
        } finally {
            if (manager != null) {
                manager.close();
            }
        }
        
    }
    
    public Boolean remove()
        throws TException
    {
        ZooTokenManager manager = null;
        try {
            String lockPath = lock.getZooPath();
            manager = ZooTokenManager.getZooTokenManager(zooConnectionString, zooNodeBase, logger);
            Boolean set = manager.removeLock(lockPath);
            return set;
            
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
            
        } finally {
            if (manager != null) {
                manager.close();
            }
        }
        
    }
    
    public Boolean verify()
        throws TException
    {
        ZooTokenManager manager = null;
        try {
            String lockPath = lock.getZooPath();
            manager = ZooTokenManager.getZooTokenManager(zooConnectionString, zooNodeBase, logger);
            Boolean set = manager.verifyLock(lockPath);
            return set;
            
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
            
        } finally {
            if (manager != null) {
                manager.close();
            }
        }
        
    }
    
    public LockItem getFlagState()
        throws TException
    {
        ZooTokenManager manager = null;
        try {
            String lockPath = lock.getZooPath();
            manager = ZooTokenManager.getZooTokenManager(zooConnectionString, zooNodeBase, logger);
            LockItem lockItem = manager.getData(lockPath);
            return lockItem;
            
        } catch (TException tex) {
            tex.printStackTrace();
            throw tex;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
            
        } finally {
            if (manager != null) {
                manager.close();
            }
        }
        
    }
    public ZooTokenState getState(String process, Boolean set)
        throws TException
    {
       return getState(process, set, null);
    }
    
    public ZooTokenState getState(String process, Boolean set, LockItem item)
        throws TException
    {
        try {
            ;
            ZooTokenState state = new ZooTokenState(
                            zooConnectionString,
                            zooNodeBase,
                            process,
                            lock,
                            set);
            
            if (item != null) {
                state.setLockCreated(item.getTimestamp());
                state.setData(item.getData());
            }
            return state;
            
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new TException(ex);
            
        }
        
    }

}
