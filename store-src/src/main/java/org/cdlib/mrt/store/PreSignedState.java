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
import java.util.Date;
import java.util.Properties;
import java.util.Vector;

import org.cdlib.mrt.s3.service.CloudResponse;
import org.cdlib.mrt.core.DateState;
import org.cdlib.mrt.utility.LinkedHashList;
import org.cdlib.mrt.utility.StateInf;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.TRuntimeException;
import org.cdlib.mrt.store.tools.StoreUtil;
import org.cdlib.mrt.utility.DateUtil;
import org.cdlib.mrt.utility.PropertiesUtil;

/**
 *
 * @author dloy
 * Provides both the Presigned URL also returns any error that may have occurred during processing
 * 
 */
public class PreSignedState
        implements StateInf, Serializable
{
    protected static final String NAME = "PreSignedState";
    protected static final String MESSAGE = NAME + ": ";

    protected URL url = null;
    protected DateState expires = null;
    protected ExceptionEnum exceptionEnum = null;
    protected Exception ex = null;

    // Presigned errors
    public enum ExceptionEnum
    {

        REQUESTED_ITEM_NOT_FOUND("file not found", 404),

        OFFLINE_STORAGE("file is in offline storage, request is not supported", 403),

        UNSUPPORTED_FUNCTION("this request not supported by this cloud service", 409),

        SERVICE_EXCEPTION("Requested format not supported", 500);

        protected final String description;
        protected final int httpResponse;

        ExceptionEnum(String description, int httpResponse) {
            this.description = description;
            this.httpResponse = httpResponse;
        }
        public String getDescription()
        {
            return description;
        }
        public int getHttpResponse()
        {
            return httpResponse;
        }
    }

    /**
     * @return new PreSignedState without content
     * @throws TException 
     */
    public static PreSignedState getPreSignedState()
        throws TException
    { 
        return new PreSignedState();
    }


    /**
     * Return PreSignedState with url and expires content
     * @param url presigned URL
     * @param expires DateState for expiration
     * @return
     * @throws TException 
     */
    public static PreSignedState getPreSignedState(String url, DateState expires)
        throws TException
    {
        return  new PreSignedState(url, expires);
    }
    
    /**
     * constructor empty state
     * @throws TException 
     */
    protected PreSignedState()
        throws TException
    {  }
    
    /**
     * Construct with url and expires content
     * @param url presigned URL
     * @param expires DateState for expiration
     * @throws TException 
     */
    protected PreSignedState(String url, DateState expires)
        throws TException
    {
        setUrl(url);
        this.expires = expires;
        System.out.println(MESSAGE 
                + " - uri:" + url
                + " - expires:" + expires.getIsoDate()
        );
    }

    /**
     * @return presigned URL as string
     */
    public String getUrl() {
        if (url != null) {
            return url.toString();
        }
        return null;
    }
    
    /**
     * @return presigned URL as URL
     */
    public URL retrieveUrl() {
        return url;
    }   

    /**
     * Set presigned URL from String
     * @param urlS string URL
     * @throws TException 
     */
    public void setUrl(String urlS) 
            throws TException
    {
        try {
            if (urlS != null) {
                this.url = new URL(urlS);
            }
        } catch (Exception ex) {
            throw new TException.INVALID_DATA_FORMAT("Exception=" + ex);
        }
    }

    /**
     * Set presigned URL using URL
     * @param url set URL
     */
    public void setUrl(URL url) {
        this.url = url;
    }

    /**
     * Return DateState for expiration
     * @return 
     */
    public DateState retrieveExpires() {
        return expires;
    }

    /**
     * @return Display Expiration as standard ISO
     */
    public String getExpires() {
        if (expires == null) {
            return null;
        }
        return expires.getIsoDate();
    }

    /**
     * Set expiration date as DateState
     * @param expires  expiration date
     */
    public void setExpires(DateState expires) {
        this.expires = expires;
    }

    /**
     * Set expiration date based on the duration for the expiration
     * @param addExpireMinutes number of minutes before the presigned expires after generation
     */
    public void setExpires(long addExpireMinutes) {
        long millSecs = addExpireMinutes * 60 * 1000;
        Date date =  DateUtil.getCurrentDatePlus(millSecs);
        this.expires = new DateState(date);
    }

    public ExceptionEnum getExceptionEnum() {
        return exceptionEnum;
    }

    /**
     * Set ExceptionEnum if an Exception occurs during Pre-Signed creation
     * @param exceptionEnum 
     */
    public void setExceptionEnum(ExceptionEnum exceptionEnum) {
        this.exceptionEnum = exceptionEnum;
    }

    /**
     * get Exception if not controlled by ExceptionEnum
     * @return 
     */
    public Exception getEx() {
        return ex;
    }

    public void setEx(Exception ex) {
        this.ex = ex;
    }

    
    public String dump(String header)
    {
        return header + ":"
                    + " - uri=" + getUrl()
                    + " - expires=" + getExpires()
                    ;
    }

    /**
     * Create a Java Properties based on successful completion
     * @return 
     */
    public Properties getProperties()
    {
        Properties prop = new Properties();
        prop.setProperty("status", "200");
        if (expires != null) {
            prop.setProperty("expires", getExpires());
        }
        if (url != null) {
            prop.setProperty("url", getUrl());
        }
        return prop;
    }

    /**
     * Create Java Properties if failed completion
     * @return 
     */
    public Properties getErrorProperties()
    {
        Properties prop = new Properties();
        int status = 0;
        String message = null;
        if (exceptionEnum == null) return prop;
        status = exceptionEnum.getHttpResponse();
        message = exceptionEnum.getDescription();
        if ((status == 500) && (ex != null)) {
            message = ex.getMessage();
        }
        prop.setProperty("status", "" + status);
        prop.setProperty("message", message);
        return prop;
    }
}

