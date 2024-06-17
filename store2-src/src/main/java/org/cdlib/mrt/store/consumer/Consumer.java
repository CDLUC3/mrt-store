/*
Copyright (c) 2021, Regents of the University of California
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

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
*********************************************************************/
package org.cdlib.mrt.store.consumer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.ConnectionLossException;
import org.apache.zookeeper.KeeperException.SessionExpiredException;

import org.cdlib.mrt.store.storage.StorageServiceInf;
import org.cdlib.mrt.store.app.StorageServiceInit;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.store.StorageConfig;
import org.cdlib.mrt.store.action.TokenRun;
import org.json.JSONObject;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.RejectedExecutionException;

import javax.servlet.*;
import javax.servlet.http.*;
import java.lang.Long;
import java.lang.IllegalArgumentException;
import java.util.NoSuchElementException;
import java.net.InetAddress;  
import java.net.UnknownHostException; 
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.zk.Access;
import org.cdlib.mrt.zk.AccessState;
import org.cdlib.mrt.zk.QueueItem;
import org.cdlib.mrt.zk.QueueItemHelper;

/**
 * Consume queue data and submit to store service
 * - zookeeper is the defined queueing service
 * 
 */
public class Consumer extends HttpServlet
{

    private static final String NAME = "Consumer";
    private static final String MESSAGE = NAME + ": ";
    private volatile Thread consumerThread = null;
    private volatile Thread cleanupThread = null;

    private static final Logger log4j = LogManager.getLogger();
   
    public static String queueConnectionString = "localhost:2181";	// default single server connection
    public static final Access.Queues  queueNodeSmall = Access.Queues.small;
    public static final Access.Queues queueNodeLarge = Access.Queues.large;
    private Access.Queues queueNode = null;	// Consumer will process this node
    public static int queueTimeOut = 400000;
    boolean largeWorker = true;
    private int numThreads = 5;		// default size
    private int pollingInterval = 2;	// default interval (minutes)
    public static long queueSizeLimit = 500000000;	// default size for large/small worker (bytes)

    public void init(ServletConfig servletConfig)
            throws ServletException {
        super.init(servletConfig);

	String queueConnectionString = null;
	String numThreads = null;
	String pollingInterval = null;
	String queueSizeLimit = null;
        StorageServiceInit storageServiceInit = null;
        StorageServiceInf storageService = null;
        StorageConfig storageConfig = null;

	try {
            storageServiceInit = StorageServiceInit.getStorageServiceInit(servletConfig);
            storageService = storageServiceInit.getStorageService();
            storageConfig = storageService.getStorageConfig();
            
	} catch (Exception e) {
	    System.err.println("[warn] " + MESSAGE + "Could not create store service in daemon init. ");
	}

	try {
            queueConnectionString = storageService.getStorageConfig().getQueueService();
	    if (StringUtil.isNotEmpty(queueConnectionString)) {
	    	System.out.println("[info] " + MESSAGE + "Setting queue connection string: " + queueConnectionString);
		this.queueConnectionString = queueConnectionString;
	    }
	} catch (Exception e) {
	    System.err.println("[warn] " + MESSAGE + "Could not set queue connection string: " + queueConnectionString +
		 "  - using default: " + this.queueConnectionString);
	}

	try {
	    queueSizeLimit = storageService.getStorageConfig().getQueueSizeLimit();
	    if (StringUtil.isNotEmpty(queueSizeLimit)) {
	    	System.out.println("[info] " + MESSAGE + "Setting queue size limit: " + queueSizeLimit);
                this.queueSizeLimit = new Long(queueSizeLimit).longValue();
	    }
	} catch (Exception e) {
	    e.printStackTrace(System.err);
	    System.err.println("[warn] " + MESSAGE + "Could not set queue size limit: " + queueNode +
		 "  - using default: " + this.queueSizeLimit);
	}

	try {
	    String hostname = getHostname();
	    String largeWorkerDef = storageService.getStorageConfig().getQueueLargeWorker();
            System.out.println("largeWorkerDef:" + largeWorkerDef
                    + " - hostname:" + hostname
            );
            if (largeWorkerDef.contains("localhost")) largeWorker = true;
            else largeWorker = largeWorkerDef.contains(hostname);
	    numThreads = storageService.getStorageConfig().getQueueNumThreadsSmall();
	    if (largeWorker) {
		numThreads = storageService.getStorageConfig().getQueueNumThreadsLarge(); 
	        queueNode = Access.Queues.large;
	    	System.out.println("[info] " + MESSAGE + "Large worker detected: " + hostname);
	    } else {
	    	queueNode = Access.Queues.small;;
	    	System.out.println("[info] " + MESSAGE + "Small worker detected: " + hostname);
	    }
	    if (StringUtil.isNotEmpty(numThreads)) {
	    	System.out.println("[info] " + MESSAGE + "Setting thread pool size: " + numThreads);
		this.numThreads = Integer.parseInt(numThreads);
	    }
	    if (queueNode == null) {
	    	System.out.println("[info] " + MESSAGE + "Setting consumer queue to: " + queueNode);
	    }
	} catch (Exception e) {
	    System.err.println("[warn] " + MESSAGE + "Could not set thread pool size: " + numThreads + "  - using default: " + this.numThreads);
	}

	try {
	    pollingInterval = storageService.getStorageConfig().getQueuePollingInterval();
	    if (StringUtil.isNotEmpty(pollingInterval)) {
	    	System.out.println("[info] " + MESSAGE + "Setting polling interval: " + pollingInterval);
		this.pollingInterval = Integer.parseInt(pollingInterval);
	    }
	} catch (Exception e) {
	    System.err.println("[warn] " + MESSAGE + "Could not set polling interval: " + pollingInterval + "  - using default: " + this.pollingInterval);
	}

        String queueTimeOutS = null;
	try {
	    queueTimeOutS = storageService.getStorageConfig().getQueueTimeout();
	    if (StringUtil.isNotEmpty(queueTimeOutS)) {
	    	System.out.println("[info] " +  MESSAGE + "Setting queue time out: " + queueTimeOutS);
		this.queueTimeOut = Integer.parseInt(queueTimeOutS);
	    }
	} catch (Exception e) {
	    System.err.println("[warn] " + MESSAGE + "Could not set queue time out: " + queueTimeOutS + "  - using default: " + this.pollingInterval);
	}

        try {
            // Start the Consumer thread
            if (consumerThread == null) {
	    	System.out.println("[info] " + MESSAGE + "starting consumer daemon");
		startConsumerThread(servletConfig);
	    }
        } catch (Exception e) {
	    throw new ServletException("[error] " + MESSAGE + "could not start consumer daemon");
        }

        try {
            // Start the Queue cleanup thread
            if (cleanupThread == null) {
	    	System.out.println("[info] " + MESSAGE + "Normal cleanup daemon would occur here");
	    }
        } catch (Exception e) {
	    throw new ServletException("[error] " + MESSAGE + "could not queue cleanup daemon");
        }

        // initialize Access
        try {
            ZooKeeper zooKeeper = storageConfig.getZooKeeper();
            log4j.debug("Consumer - Access initialized");
            
        } catch (Exception e) {
	    throw new ServletException("[error] " + MESSAGE + "could not queue cleanup daemon");
        }
    }


    /**
     * Start consumer thread
     */
    private synchronized void startConsumerThread(ServletConfig servletConfig)
        throws Exception
    {
        try {
            if (consumerThread != null) {
                System.out.println("[info] " + MESSAGE + "consumer daemon already started");
                return;
            }

            ConsumerDaemon consumerDaemon = new ConsumerDaemon(queueConnectionString, queueNode,
		servletConfig, queueTimeOut, pollingInterval, numThreads, largeWorker, queueNodeSmall);

            consumerThread =  new Thread(consumerDaemon);
            consumerThread.setDaemon(true);                // Kill thread when servlet dies
            consumerThread.start();

	    System.out.println("[info] " + MESSAGE + "consumer daemon started");

            return;

        } catch (Exception ex) {
            throw new Exception(ex);
        }
    }
    
    public String getName() {
        return NAME;
    }

    public String getHostname() {
        try {  
           InetAddress id = InetAddress.getLocalHost();  
	   return id.getHostName();
        } catch (UnknownHostException e) {  
	   System.err.println("[error] " + MESSAGE + "could not determine hostname.");
	   return null;
        }  
    }

    public void destroy() {
	try {
	    System.out.println("[info] " + MESSAGE + "interrupting consumer daemon");
            consumerThread.interrupt();
	    saveState();
	} catch (Exception e) {
	    e.printStackTrace(System.err);
	}
    }

    public void saveState() {
        System.out.println("[info] " + MESSAGE + "Shutdown detected, saving state, if applicable.");
    }

}


class ConsumerDaemon implements Runnable
{
   
    private static final String NAME = "ConsumerDaemon";
    private static final String MESSAGE = NAME + ": ";

    private static final Logger log4j = LogManager.getLogger();
    
    private StorageServiceInit storageServiceInit = null;
    private StorageServiceInf storageService = null;

    private String queueConnectionString = null;
    //private Access.Queues queueNode = null;
    //private Access.Queues queueSmallNode = null;
    private Integer pollingInterval = null;
    private Integer poolSize = null;
    private Integer queueTimeOut = 40000;
    private boolean largeWorker = false;
    int smallQueueCounter = 0;
    boolean smallQueueBool = true;

    private ZooKeeper zooKeeper = null;
    //private ZooTokenManager lockManager = null;
    //private DistributedQueue distributedQueue = null;
    //private DistributedQueue distributedSmallQueue = null;
    private String largeAccessHold = null;
    private String smallAccessHold = null;

    // session data
    private long sessionID;
    private byte[] sessionAuth;
    private StorageConfig storageConfig = null;

    // Constructor
    public ConsumerDaemon(String queueConnectionString, Access.Queues queueNode, ServletConfig servletConfig, 
		Integer queueTimeOut, Integer pollingInterval, Integer poolSize, boolean largeWorker, Access.Queues queueNodeSmall)
    {
        this.queueConnectionString = queueConnectionString;
        //this.queueNode = queueNode;
        //this.queueSmallNode = queueNodeSmall;
	this.queueTimeOut = queueTimeOut;
	this.pollingInterval = pollingInterval;
	this.poolSize = poolSize;
	this.largeWorker = largeWorker;

	try {
            storageServiceInit = StorageServiceInit.getStorageServiceInit(servletConfig);
            storageService = storageServiceInit.getStorageService();;
            
            storageConfig = storageService.getStorageConfig();
            zooKeeper = storageConfig.getZooKeeper();
	    LoggerInf mrtLogger = storageService.getLogger();

	} catch (Exception e) {
	    e.printStackTrace(System.err);
	}
    }

    public void run()
    {
        boolean init = true;
        String status = null;
        ArrayBlockingQueue<ConsumeData> workQueue = new ArrayBlockingQueue<ConsumeData>(poolSize);
        ThreadPoolExecutor executorService = new ThreadPoolExecutor(poolSize, poolSize, (long) 5, TimeUnit.SECONDS, (BlockingQueue) workQueue);

	sessionID = zooKeeper.getSessionId();
	System.out.println("[info]" + MESSAGE + "session id: " + Long.toHexString(sessionID));
	sessionAuth = zooKeeper.getSessionPasswd();
        log4j.info("ConsumerDaemon Start"
                + " - poolSize=" + poolSize
                + " - smallQueueBool=" + smallQueueBool
                + " - largeWorker=" + largeWorker
        );

        try {
            //long queueSize = workQueue.size();
            while (true) {      // Until service is shutdown

                // Wait for next interval.
                if (! init) {
                    System.out.println(MESSAGE + "Waiting for polling interval(seconds): " + pollingInterval);
                    Thread.yield();
                    Thread.currentThread().sleep(pollingInterval.longValue() * 1000);
		    smallQueueCheck();
                } else {
                    init = false;
                }

                // Let's check to see if we are on hold
                if (onHold()) {
                    System.out.println(MESSAGE + "onHold set.");
                    continue;
                }

                // have we shutdown?
                if (Thread.currentThread().isInterrupted()) {
                    System.out.println(MESSAGE + "interruption detected.");
      		    throw new InterruptedException();
                }

		// Perform some work
		try {
		    long numActiveTasks = 0;

		    // To prevent long shutdown, no more than poolsize tasks queued.
                    boolean itemToProcess = false;
		    do {
                        itemToProcess = false;
		        numActiveTasks = executorService.getActiveCount();
			if (largeWorker && smallQueueBool && numActiveTasks == 0) {
			    System.out.println(MESSAGE + "Checking for additional Access tasks on SMALL queue.  Current tasks: " + numActiveTasks + " - Max: " + poolSize);
			    smallQueueBool = false;
			    smallQueueCounter = 0;       
                            Access aa = Access.acquirePendingAssembly(zooKeeper, Access.Queues.small);
                            if (aa != null) {
                                log4j.info("Access.acquirePendingAssembly:" + aa.id());
                                executorService.execute(new ConsumeData(storageService, aa));
                                itemToProcess = true;
                            } else {
                                System.out.println("[info] " + MESSAGE + "No data in queue to process");
                            }
			}
                        
			if (numActiveTasks < poolSize) {
			    System.out.println(MESSAGE + "Checking for additional Access tasks.  Current tasks: " + numActiveTasks + " - Max: " + poolSize);
                            if (largeWorker) {
                                Access aa = Access.acquirePendingAssembly(zooKeeper, Access.Queues.large);
                                if (aa != null) {
                                    log4j.info("Access.acquirePendingAssembly:" + aa.id());
                                    executorService.execute(new ConsumeData(storageService, aa));
                                    itemToProcess = true;
                                } else {
                                    System.out.println("[info] " + MESSAGE + "No data in large queue to process");
                                }
                            } else {
                                Access aa = Access.acquirePendingAssembly(zooKeeper, Access.Queues.small);
                                if (aa != null) {
                                    log4j.info("Access.acquirePendingAssembly:" + aa.id());
                                    executorService.execute(new ConsumeData(storageService, aa));
                                    itemToProcess = true;
                                } else {
                                    System.out.println("[info] " + MESSAGE + "No data in small queue to process");
                                }
                            }
			} else {
			    System.out.println(MESSAGE + "Work queue is full, NOT checking for additional Access tasks: " + numActiveTasks + " - Max: " + poolSize);
			    break;
			}
                        log4j.trace("itemToProcess:" + itemToProcess);
		    } while (itemToProcess) ;
                    //System.out.println("[info] " + MESSAGE + "No data in queue to process");
                    
        	} catch (ConnectionLossException cle) {
		    System.err.println("[error] " + MESSAGE + "Lost connection to queueing service.");
		    cle.printStackTrace(System.err);
                    log4j.warn("Lost connection to queueing service.");
                    
        	} catch (SessionExpiredException see) {
		    see.printStackTrace(System.err);
		    System.err.println("[warn] " + MESSAGE + "Session expired.  Attempting to recreate session.");
            	    zooKeeper = storageConfig.getZooKeeper();
                    log4j.warn("Session expired.  Attempting to recreate session.");
                    
		} catch (RejectedExecutionException ree) {
	            System.out.println("[info] " + MESSAGE + "Thread pool limit reached. no submission, and requeuing: " + ree);
                    log4j.warn("Thread pool limit reached. no submission, and requeuing: " + ree);
        	    Thread.currentThread().sleep(5 * 1000);         // let thread pool relax a bit
                    
		} catch (NoSuchElementException nsee) {
		    // no data in queue
		    System.out.println("[info] " + MESSAGE + "No data in queue to process");
                    //log4j.warn("No data in queue to process");
                    
		} catch (IllegalArgumentException iae) {
		    // no queue exists
		    System.out.println("[info] " + MESSAGE + "New queue does not yet exist: " + iae);
                    log4j.warn("New queue does not yet exist: " + iae);
                    
		} catch (Exception e) {
		    System.err.println("[warn] " + MESSAGE + "General exception.");
	            e.printStackTrace();
                    log4j.error("General exception:" + e);
		}
	    }
        } catch (InterruptedException ie) {
	    try {
		try {
	    	    zooKeeper.close();
		} catch (Exception ze) {}
                System.out.println(MESSAGE + "shutting down consumer daemon.");
                log4j.info("hutting down consumer daemon.");
	        executorService.shutdown();

		int cnt = 0;
		while (! executorService.awaitTermination(15L, TimeUnit.SECONDS)) {
                    System.out.println(MESSAGE + "waiting for tasks to complete.");
		    cnt++;
		    if (cnt == 8) {	// 2 minutes
			// force shutdown
	        	executorService.shutdownNow();
		    }
		}
            } catch (Exception e) {
		e.printStackTrace(System.err);
            }
	} catch (Exception e) {
            System.out.println(MESSAGE + "Exception detected, shutting down consumer daemon.");
	    e.printStackTrace(System.err);
	    executorService.shutdown();
        } finally {
	}
    }

    // simple modulo method
    private void smallQueueCheck()
    {
	smallQueueCounter++;
	if ((smallQueueCounter % 4) == 0) smallQueueBool = !smallQueueBool;
    }

    // to do: make this a service call
    
    private boolean onHold()
    {
        try {
            if (largeWorker) {
                if (QueueItemHelper.exists(zooKeeper, QueueItem.ZkPaths.LocksQueueAccessLarge.path)) {
                    System.out.println("[info]" + NAME + ": largeAccessHold lock exists, not processing queue");
                    log4j.info("largeAccessHold lock exists, not processing queue");
                    return true;
                }
                
            } else {
                if (QueueItemHelper.exists(zooKeeper, QueueItem.ZkPaths.LocksQueueAccessSmall.path)) {
                    System.out.println("[info]" + NAME + ": smallAccessHold lock exists, not processing queue");
                    log4j.info("smallAccessHold lock exists, not processing queue");
                    return true;
                }
            }
        } catch (Exception e) {
            return false;
        }
        return false;
    }

   public class Ignorer implements Watcher {
       public void process(WatchedEvent event){
           if (event.getState().equals("Disconnected"))
               System.out.println("Disconnected: " + event.toString());
       }
   }

}


class ConsumeData implements Runnable
{
    private static final Logger log4j = LogManager.getLogger();
   
    private static final String NAME = "ConsumeData";
    private static final String MESSAGE = NAME + ":";
    private static final boolean DEBUG = true;
    protected static final String FS = System.getProperty("file.separator");

    private String queueConnectionString = null;
    private String queueNode = null;
    private ZooKeeper zooKeeper = null;
    private int queueTimeout = 40000;

    private Access access = null;
    private StorageServiceInf storageService = null;
    private StorageConfig config = null;

    // Constructor
    public ConsumeData(StorageServiceInf storageService, Access access)
    {
	this.access = access;
	this.storageService = storageService;
        config = storageService.getStorageConfig();
        this.queueConnectionString = config.getQueueService();
        String queueTimeoutS = config.getQueueTimeout();
        try {
            queueTimeout = Integer.parseInt(queueTimeoutS);
        } catch (Exception e) {
            queueTimeout = 40000;
        }
    }

    public void run()
    {
        String token = null;
        try {
            zooKeeper = config.getZooKeeper();
	    JSONObject jo = access.data();
	    token = jo.getString("token");
            System.out.println("JO DUMP:" + jo.toString(2));

	    TokenRun tokenRun = TokenRun.getTokenRun(jo.toString(), storageService.getStorageConfig());
            access.setStatus(zooKeeper, AccessState.Processing);
            
            log4j.debug("access run:" + access.id() + " - token:" + token);
	    tokenRun.run();
            log4j.debug("************STATUS before*********\n"
                    + " - access.id():" + access.id() + "\n"
                    + " - access.status():" + access.status() + "\n"
                    + " - access.isDeletable()():" + access.status().isDeletable() + "\n"
            );
	    if (tokenRun.getRunStatus() != TokenRun.TokenRunStatus.OK) {
                if (DEBUG) System.out.println("[error] TokenRun error:" + tokenRun.getRunStatus());
                log4j.info("access.setStatus fail id:" + access.id() + " - token:" + token);
	        access.setStatus(zooKeeper, access.status().fail());
                access.unlock(zooKeeper);
            } else {
		// complete or delete??
            	if (DEBUG) System.out.println("[item] END: completed queue data:" + jo.toString(2));
                log4j.info("access.setStatus OK id:" + access.id() + " - token:" + token);
	        access.setStatus(zooKeeper, access.status().success());
                access.unlock(zooKeeper);
	    }
            log4j.debug("************STATUS after*********\n"
                    + " - access.id():" + access.id() + "\n"
                    + " - access.status():" + access.status() + "\n"
                    + " - access.isDeletable()():" + access.status().isDeletable() + "\n"
            );
        }  catch (Exception e) {
            e.printStackTrace(System.err);
            System.out.println("[error] Consuming queue data:" + e);
            log4j.error("access.setStatus fail" + access.id() + " - token:" + token + " - exception:" + e);
            try {
                access.setStatus(zooKeeper, access.status().fail());
                access.unlock(zooKeeper);
            } catch (Exception ze) {
                System.out.println("Unable to set acccee status - Zookeeper exception:" + ze);
            }
                
        } finally {
	} 
    }

   public class Ignorer implements Watcher {
       public void process(WatchedEvent event){
           if (event.getState().equals("Disconnected"))
               System.out.println("Disconnected: " + event.toString());
       }
   }
}