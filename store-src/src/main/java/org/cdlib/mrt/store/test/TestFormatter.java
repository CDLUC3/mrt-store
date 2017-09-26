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
*********************************************************************/
package org.cdlib.mrt.store.test;

import org.cdlib.mrt.store.can.CAN;
import org.cdlib.mrt.store.can.CANAbs;
import java.io.File;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

import org.cdlib.mrt.core.FileComponent;
import org.cdlib.mrt.store.NodeState;
import org.cdlib.mrt.store.VersionContent;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.utility.TFrame;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StringUtil;


/**
 *
 * @author dloy
 */
public class TestFormatter
{


    protected static final String NAME = "TestFormatter";
    protected static final String MESSAGE = NAME + ": ";
    protected static LoggerInf logger = null;

    /**
     * Main method
     */
    public static void main(String args[])
    {
        TFrame framework = null;
        try
        {
            String propertyList[] = {
                "resources/MFrameDefault.properties",
                "resources/MFrameService.properties",
                "resources/FeederService.properties",
                "resources/IngestClient.properties",
                "resources/FeederClient.properties",
                "resources/MFrameLocal.properties"};
            framework = new TFrame(propertyList, NAME);
            logger = framework.getLogger();

            String storeName = framework.getProperty(NAME + ".store");
            log("storeName=" + storeName, 0);
            File storeFile = new File(storeName);
            if (!storeFile.exists()) {
                log("storeFile does not exist", 0);
            }

            String objectIDS = framework.getProperty(NAME + ".objectID");
            log("objectIDS=" + objectIDS, 0);
            int nodeID =100;
            CAN can = CANAbs.getCAN(logger, storeName);
            Identifier objectID = new Identifier(objectIDS);
            System.out.println(MESSAGE + "begin processing:");
            testGetVersion(can, objectID);

        }  catch(Exception e)  {
            if (framework != null)
            {
                framework.getLogger().logError(
                    "Main: Encountered exception:" + e, 0);
                framework.getLogger().logError(
                        StringUtil.stackTrace(e), 10);
            }
        }
    }

    protected static void testGetVersion(
            CAN can,
            Identifier objectID)
    {
        try {
            log(MESSAGE + "before xxx testPutVersion", 0);
            VersionContent versionContent = can.getVersionContent(objectID, 0);
            dumpState("versionState2",versionContent);
            dumpCANState("CAN State", can);
            format(versionContent);

        } catch (TException ex) {
            log(MESSAGE + "Exception:" + ex, 0);
            log(MESSAGE + "Trace:" + StringUtil.stackTrace(ex), 0);
        }
    }


    public static void format(Object obj)
    {
        printStart();
        formatNode(obj);
        printClose();
    }

    public static void formatNode(Object obj)
    {
        try {
            String name = getObjectName(obj);
            printOpenNode(name);
            formatObject(obj);
            printCloseNode(name);

        } catch (Exception ex) {
            log(MESSAGE + "Exception:" + ex, 0);
            log(MESSAGE + "Trace:" + StringUtil.stackTrace(ex), 0);
        }
    }

    protected static void dumpState(String header, VersionContent versionState)
    {
        log("************dumpState " + header + " ***************", 0);
        LinkedHashMap<String, FileComponent> versionFiles = versionState.getVersionTable();
        Set<String> fileSet = versionFiles.keySet();
        for (String key : fileSet) {
            if (StringUtil.isEmpty(key)) continue;
            FileComponent fileState = versionFiles.get(key);
            if (fileState == null) continue;
            log(MESSAGE + fileState.dump("***" + key + "***:"), 0);
        }

    }

    protected static void dumpCANState(String header, CAN can)
        throws TException
    {
        log("************CANSTATE " + header + " ***************", 0);
        NodeState nodeState = can.getNodeState();
        log(nodeState.dump("*******TestCAN********"), 0);

    }

    protected static void formatter(String header, VersionContent versionState)
    {
        log("************Formatter " + header + " ***************", 0);
         try {
            Class c = versionState.getClass();
            if (!isStateClass(c)) return;
            //Class c = Class.forName(versionState);
            Method marr[] = c.getDeclaredMethods();
            Method m = null;


            for (int i = 0; i < marr.length; i++) {
                m = marr[i];
                System.out.println("***method***" + m.toString());
                if (isDisplayMethod(m)) {
                    display(m, versionState);
                } else if (returnsList(m)) {
                    System.out.println("---->list-" + m.getName());
                    processList(m, versionState);
                }
            }

            formatObject(versionState);
         }
         catch (Throwable e) {
            System.err.println(e);
         }

    }

    protected static void formatObject(Object object)
    {
         try {
            Class c = object.getClass();
            if (!isStateClass(c)) return;
            //Class c = Class.forName(versionState);
            Method marr[] = c.getDeclaredMethods();
            Method m = null;


            for (int i = 0; i < marr.length; i++) {
                m = marr[i];
                String test = m.toString();
                if (test.contains("private")) continue;
                if (isDisplayMethod(m)) {
                    display(m, object);
                } else if (returnsList(m)) {
                    processList(m, object);
                }
            }
         }
         catch (Throwable e) {
            System.err.println(e);
         }

    }
    public static void display(Method m, Object obj)
            throws Exception
    {
        String name = m.getName();
        name = name.substring(3);
        String retval = runStringMethod(m, obj);
        print(name, retval);
    }

    public static void printStart()
    {
        System.out.println("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
    }
    public static void printClose()
    {
    }

    public static void printOpenNode(String name)
    {
        System.out.println("<" + name + ">");
    }

    public static void printCloseNode(String name)
    {
        System.out.println("</" + name + ">");
    }

    public static void print(String name, String value)
    {
        if (value == null) value = "";
        System.out.println("<" + name + ">"+ value + "</" + name + ">");
    }

    public static void processList(Method mGetList, Object objList)
    {
        try {
            List list = (List)runMethod(mGetList, objList);
            if (list == null) return;
            String name = mGetList.getName();
            if (name.startsWith("get")) name = name.substring(3);
            printOpenNode(name);
            for (Object obj: list) {
                formatNode(obj);
            }
            printCloseNode(name);
            return;

        } catch (Throwable e) {
            System.err.println(e);
            return;
        }
    }

    protected static String getObjectName(Object obj)
    {
        String name = obj.getClass().getName();
        int pos = name.lastIndexOf('.');
        if (pos >= 0) name = name.substring(pos + 1);
        return name;
    }
    protected static boolean isDisplayMethod(Method m)
    {
        boolean getName = false;
        boolean dispType = false;
        boolean noParm = false;
        try {
            String name = m.getName();
            if (name.startsWith("get")) getName = true;
            String returnType = m.getReturnType().getName();

            if (returnType.equals("java.lang.String")) dispType=true;
            else if (returnType.equals("java.lang.Integer")) dispType=true;
            else if (returnType.equals("java.lang.Integer")) dispType=true;
            else if (returnType.equals("java.net.URI")) dispType=true;
            else if (returnType.equals("java.net.URL")) dispType=true;
            else if (returnType.equals("int")) dispType=true;
            else if (returnType.equals("long")) dispType=true;
            else if (returnType.equals("org.cdlib.mrt.core.Identifier")) dispType=true;
            else if (returnType.equals("org.cdlib.mrt.core.MessageDigest")) dispType=true;
            else {
                /*
                try {
                    Method toString = m.getReturnType().getMethod("toString",null);
                    System.out.println("match:" + m.getName());
                    dumpAnnotations(toString);
                    //dispType=true;
                } catch (Exception ex) {
                }
                 */
            }

            Class [] params = m.getParameterTypes();
            int parmCnt = params.length;
            if (parmCnt == 0) noParm = true;
            boolean retval = noParm & getName & dispType;
            return retval;

        } catch (Throwable e) {
            System.err.println(e);
            return false;
        }

    }

    protected static Object runMethod(Method meth, Object object)
    {
        
        try {
            return meth.invoke(object, (java.lang.Object[])null);

        } catch (Throwable e) {
            System.err.println(e);
            return null;
        }

    }

    protected static String runStringMethod(Method meth, Object object)
    {

        try {
            Object retobj
              = meth.invoke(object, (java.lang.Object[])null);
            String retval = new String("" + retobj);
            return retval;

        } catch (Throwable e) {
            System.err.println(e);
            return null;
        }

    }


    protected static boolean returnsList(Method m)
    {
        try {
            String name = m.getName();
            if (!name.startsWith("get")) return false;
            Class returnType = m.getReturnType();
            if (!isList(returnType)) return false;
            return true;

        } catch (Throwable e) {
            System.err.println(e);
            return false;
        }

    }


    protected static boolean isStateClass(Class c)
    {
        return interfaceMatches(c, "org.cdlib.mrt.core.StateInf");

    }
    protected static boolean isList(Class c)
    {
        return interfaceMatches(c, "java.util.List");

    }


    protected static boolean interfaceMatches(Class c, String match)
    {
       
         try {
            Class [] interfaces = c.getInterfaces();
            for (int i = 0; i < interfaces.length; i++) {
                Class inter = interfaces[i];
                String interfaceName = inter.getName();
                if (interfaceName.equals(match)) {
                    return true;
                }
            }
            return false;
         }
         catch (Throwable e) {
            System.err.println(e);
            return false;
         }

    }

    protected static void dumpClass(Class c)
    {
        log("************ dumpClass ***************", 0);
         try {
            Class [] interfaces = c.getInterfaces();
            for (int i = 0; i < interfaces.length; i++) {
                Class inter = interfaces[i];
                System.out.println("interfaces=" + inter.getName());
            }
         }
         catch (Throwable e) {
            System.err.println(e);
         }

    }

    protected static boolean matchAnnotation(String annotationName, Method m)
    {
        try {
            System.out.println("matchAnnotation entered method=" + m.getName());
            Annotation [] annotations = m.getDeclaredAnnotations();
            System.out.println("annotation length=" + annotations.length);
            Annotation annotation = null;
            for (int i = 0; i < annotations.length; i++) {
                annotation = annotations[i];
                System.out.println("matchAnnotation::" + annotation.toString());
                if (annotation.toString().equals(annotationName)) {
                    return true;
                }
            }
            return false;
         }
         catch (Throwable e) {
            System.err.println(e);
            return false;
         }
    }

    protected static boolean dumpAnnotations( Method m)
    {
        try {
            System.out.println("matchAnnotation entered method=" + m.getName());
            Annotation [] annotations = m.getDeclaredAnnotations();
            System.out.println("annotation length=" + annotations.length);
            Annotation annotation = null;
            for (int i = 0; i < annotations.length; i++) {
                annotation = annotations[i];
                System.out.println("dumpAnnotation::" + annotation.toString());
            }
            return false;
         }
         catch (Throwable e) {
            System.err.println(e);
            return false;
         }
    }

    protected static void log(String msg, int lvl)
    {
        System.out.println(msg);
        //logger.logMessage(msg, 0, true);
    }

}