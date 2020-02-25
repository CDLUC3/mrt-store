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
 */
public class PreSignedState
        implements StateInf, Serializable
{
    protected static final String NAME = "PreSignedState";
    protected static final String MESSAGE = NAME + ": ";

    protected URL url = null;
    protected DateState expires = null;
    protected CloudResponse cloudResponse = null;
    protected ExceptionEnum exceptionEnum = null;
    protected Exception ex = null;

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
     * Factory to get StorageService State
     * @param storageProp store-info.txt properties
     * @return StorageServiceState
     * @throws TException process exception
     */
    public static PreSignedState getPreSignedState()
        throws TException
    { 
        return new PreSignedState();
    }


    /**
     * Factory to get StorageService State
     * @param storageProp store-info.txt properties
     * @return StorageServiceState
     * @throws TException process exception
     */
    public static PreSignedState getPreSignedState(String url, DateState expires)
        throws TException
    {
        return  new PreSignedState(url, expires);
    }
    
    /**
     * Return State PreSigned command
     * @param uri presigned URI
     * @param expires expiration for presigned URI
     * @throws TException 
     */
    protected PreSignedState()
        throws TException
    {  }
    
    /**
     * Return State PreSigned command
     * @param uri presigned URI
     * @param expires expiration for presigned URI
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

    public String getUrl() {
        if (url != null) {
            return url.toString();
        }
        return null;
    }
    
    public URL retrieveUrl() {
        return url;
    }   

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

    public void setUrl(URL url) {
        this.url = url;
    }

    public DateState retrieveExpires() {
        return expires;
    }

    public String getExpires() {
        if (expires == null) {
            return null;
        }
        return expires.getIsoZDate();
    }

    public void setExpires(DateState expires) {
        this.expires = expires;
    }

    public void setExpires(long addExpireMinutes) {
        long millSecs = addExpireMinutes * 60 * 1000;
        Date date =  DateUtil.getCurrentDatePlus(millSecs);
        this.expires = new DateState(date);
    }

    public CloudResponse getCloudResponse() {
        return cloudResponse;
    }

    public void setCloudResponse(CloudResponse cloudResponse) {
        this.cloudResponse = cloudResponse;
    }

    public ExceptionEnum getExceptionEnum() {
        return exceptionEnum;
    }

    public void setExceptionEnum(ExceptionEnum exceptionEnum) {
        this.exceptionEnum = exceptionEnum;
    }

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

    public Properties getProperties()
    {
        Properties prop = new Properties();
        if (expires != null) {
            prop.setProperty("expires", getExpires());
        }
        if (url != null) {
            prop.setProperty("url", getUrl());
        }
        return prop;
    }

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

