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
package org.cdlib.mrt.store.utility;

import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.ZooKeeper;
import org.apache.logging.log4j.ThreadContext;


/**
 * Zookeeper static definitions
 * @author mreyes
 */
public class ZookeeperUtil
{

    private static final String NAME = "ZookeeperUtil";
    private static final String MESSAGE = NAME + ": ";
    private static final boolean DEBUG = true;

    public static final int SLEEP_ZK_RETRY = (30 * 1000);			// 30 sec retry timeout
    public static final int ZK_SESSION_TIMEOUT = (6 * 60 * 60 * 1000); 		// 6 hour session timeout


    public static boolean validateZK(ZooKeeper zk)
    {

	Stat stat = null;
	String caller = Thread.currentThread().getStackTrace()[2].getClassName() + "." + 
			Thread.currentThread().getStackTrace()[2].getMethodName() + "() : " +
			Thread.currentThread().getStackTrace()[2].getLineNumber();
        try {
	    zk.exists("/zookeeper", null);
	    return true;
	} catch (Exception e) {
	    // Log
	    if (DEBUG) System.err.println("[INFO] ZookeeperUtil: Need to ESTABLISH/REFRESH ZK Connection " + caller);
	    ThreadContext.put("Zookeeper Connection needs refresh", caller);
	    try {
		// Close expired connection.
		zk.close();
		zk = null;
	    } catch (Exception e2) {
	    }
	    return false;
	} finally {

	}
    }

}
