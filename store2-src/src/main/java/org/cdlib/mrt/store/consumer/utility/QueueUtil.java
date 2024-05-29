/*
Copyright (c) 2011, Regents of the University of California
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
package org.cdlib.mrt.store.consumer.utility;

import java.io.IOException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.cdlib.mrt.zk.Access;
import org.cdlib.mrt.zk.MerrittZKNodeInvalid;
import org.cdlib.mrt.queue.DistributedQueue;
import org.cdlib.mrt.store.consumer.Consumer;
import org.cdlib.mrt.utility.TException;
// import javax.json.JsonNumber;
import org.json.JSONObject;
/**
 * Queue access request
 * zookeeper is the defined queueing service
 * @author mreyes
 */
public class QueueUtil 
{

    private static final Logger log4j = LogManager.getLogger();
    
    protected static final String NAME = "QueueUtil";
    protected static final String MESSAGE = NAME + ": ";
    protected static final boolean DEBUG = true;
    private static ZooKeeper zooKeeper;
    //private static DistributedQueue distributedQueue;
    private static Access distributedAccess;
    private static Access.Queues  queueNode = null;


    /**
     * Submit access request to appropriate node
     *
     * @param request JSON request parms (status, token, size, delivery-node)
     * @return status of queue request action
     */
    public static boolean queueAccessRequest(String request)
	throws TException 
    {
        try {
	    // sample request: {"status":201,"token":"bf6ef133-9b05-4fd8-a4d1-845c40125315","cloud-content-byte":206290233,"delivery-node":7001}
	    JSONObject jo = string2json(request);
            queueNode = Access.Queues.small;
	    long size = jo.getLong("cloud-content-byte");
	    String token = jo.getString("token");
	    if (size  > Consumer.queueSizeLimit) {
		System.out.println("[info]" + MESSAGE + NAME + " Detected LARGE access request: " + token);
	        queueNode = Access.Queues.large;
	    } else {
		System.out.println("[info]" + MESSAGE + NAME + " Detected SMALL access request: " + token);
	    }
		
            zooKeeper = new ZooKeeper(Consumer.queueConnectionString, DistributedQueue.sessionTimeout, new Ignorer());
	    String priority = calculatePriority(size);
            distributedAccess = Access.createAssembly(zooKeeper, queueNode, jo);

            String msg = String.format("SUCCESS: %s completed successfully", getName());
            log4j.info(msg);
            return true;
            
        } catch (IOException ex) {
            String msg = String.format("WARNING: %s could not connect to Zookeeper", getName());
            log4j.warn(msg);
            return false;
            
        } catch (KeeperException ex) {
            String msg = String.format("WARNING: %s %s.", getName(), ex);
            log4j.warn(msg);
            return false;
            
        } catch (InterruptedException ex) {
            String msg = String.format("WARNING: %s %s.", getName(), ex);
            log4j.warn(msg);
            return false;
            
        } catch (org.json.JSONException ex) {
            String msg = String.format("WARNING: %s %s.", getName(), ex);
            log4j.warn(msg);
            return false;
            
        } catch (MerrittZKNodeInvalid ex) {
            String msg = String.format("WARNING: %s %s.", getName(), ex);
            log4j.warn(msg);
            return false;   
            
        } finally {
            try { zooKeeper.close(); }
            catch (Exception ex) {}
        }
    }

    public static String getName() {
	return NAME;
    }

    private static String calculatePriority(long size) {
	return "00";	//default
    }

    public static JSONObject string2json(String jsonObjectString)
        throws TException
    {
        JSONObject jsonObject = null;
        try {
            jsonObject = new JSONObject(jsonObjectString);
        } catch (Exception e) { }

        return jsonObject;
    }


   public static class Ignorer implements Watcher {
        public void process(WatchedEvent event){}
   }


}
