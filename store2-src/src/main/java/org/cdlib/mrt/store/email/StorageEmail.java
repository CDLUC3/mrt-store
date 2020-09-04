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
*********************************************************************/
package org.cdlib.mrt.store.email;

import org.cdlib.mrt.store.email.*;
import java.util.*;
import java.io.*;
//import javax.mail.*;
import javax.mail.Message;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMultipart;
import javax.mail.internet.MimeMessage;
import org.cdlib.mrt.formatter.FormatInfo;
import org.cdlib.mrt.utility.TException;
import org.jdom.Document;
import org.jdom.input.SAXBuilder;

import org.jdom.Attribute;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.input.SAXBuilder;
import org.jdom.output.XMLOutputter;
import org.jdom.xpath.XPath;

import org.cdlib.mrt.cloud.ManInfo;
import org.cdlib.mrt.cloud.VersionMap;
import org.cdlib.mrt.core.ComponentContent;
import org.cdlib.mrt.core.DateState;
import org.cdlib.mrt.core.FileComponent;
import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.core.MessageDigest;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.TFileLogger;
import org.jdom.Namespace;
import org.jdom.output.Format;
/**
* Simple demonstration of using the javax.mail API.
*
* Run from the command line. Please edit the implementation
* to use correct email addresses and host name.
*/
public final class StorageEmail
{
    private static final String NAME = "StorageEmail";
    private static final String MESSAGE = NAME + ": ";
    private static final boolean DEBUG = false;
    
    protected Session session = null;
    protected ArrayList<String> recipientsBCC = new ArrayList<>();
    public StorageEmail(Properties emailProp)
    {
        setSession(emailProp, true);
    }

    public static void main( String... aArguments )
    {
        try {
            Properties props = new Properties();
            props.put("mail.smtp.host", "smtp.ucop.edu");
            StorageEmail emailer = new StorageEmail(props);
            String[] to = {"dloy@ucop.edu"};
            emailer.sendEmail(
                to,
                "dloy@ucop.edu",
                "Testing 1-2-3",
                "this is a test - this is only a test"
            );
            
        } catch (Exception ex) {
            System.out.println("Exception:" + ex);
            ex.printStackTrace();
        }
    }

    /**
    * Send a single part email.
    */
    public void sendEmail(
            String recipients[ ],
            String from,
            String subject,
            String msg)
        throws TException
    {
        MimeMessage message = new MimeMessage( session );
        try {

            // set the from and to address
            InternetAddress addressFrom = new InternetAddress(from);
            message.setFrom(addressFrom);

            if (DEBUG)System.out.println("recepients length=" + recipients.length);
            InternetAddress[] addressTo = new InternetAddress[recipients.length];
            for (int i = 0; i < recipients.length; i++)
            {
                addressTo[i] = new InternetAddress(recipients[i]);
            }
            message.setRecipients(Message.RecipientType.TO, addressTo);          
            if (recipientsBCC.size() > 0) {
                if (DEBUG) System.out.println("BCC recipients length=" + recipientsBCC.size());
                InternetAddress[] addressBCC = new InternetAddress[recipientsBCC.size()];
                for (int i = 0; i < recipientsBCC.size(); i++)
                {
                    addressBCC[i] = new InternetAddress(recipientsBCC.get(i));
                }
                message.setRecipients(Message.RecipientType.BCC, addressBCC);
            }
            message.setSubject( subject );
            message.setSentDate(new Date());
            message.setText( msg );
            Transport.send( message );

        } catch (Exception ex){
            System.out.println("sendEmail Exception:" + ex);
            ex.printStackTrace();
            throw new TException(ex);
        }
    }

    /**
    * Send a single email.
    */
    public void sendEmail(
            String name,
            String formatType,
            String recipients[ ],
            String from,
            String subject,
            String msg,
            String body)
        throws TException
    {
        MimeMessage message = new MimeMessage( session );
        try {
            formatType = formatType.toLowerCase();
            FormatInfo info = FormatInfo.valueOf(formatType);
            String mime = info.getMimeType();
            if (mime.contains("html")) mime = "text/html";
            String bodyContentType = mime + "; charset=\"utf-8\"";

            String ext = info.getExtension();
            if (mime.contains("html")) ext = "html";
            String bodyFileName = name + "." + ext;
            
            // set the from and to address
            InternetAddress addressFrom = new InternetAddress(from);
            message.setFrom(addressFrom);

            if (DEBUG) System.out.println("recipients length=" + recipients.length);
            InternetAddress[] addressTo = new InternetAddress[recipients.length];
            for (int i = 0; i < recipients.length; i++)
            {
                addressTo[i] = new InternetAddress(recipients[i]);
            }
            message.setRecipients(Message.RecipientType.TO, addressTo);
            
            if (recipientsBCC.size() > 0) {
                if (DEBUG) System.out.println("BCC recipients length=" + recipientsBCC.size());
                InternetAddress[] addressBCC = new InternetAddress[recipientsBCC.size()];
                for (int i = 0; i < recipientsBCC.size(); i++)
                {
                    String recipientBCC = recipientsBCC.get(i);
                    if (DEBUG) System.out.println("recipientBCC:" + recipientBCC); //!
                    addressBCC[i] = new InternetAddress(recipientBCC);
                }
                message.setRecipients(Message.RecipientType.BCC, addressBCC);
            }
            
            message.setSentDate(new Date());
            message.setSubject( subject );
      
            MimeMultipart multipart = new MimeMultipart();

            if (!StringUtil.isAllBlank(msg)) {      
                MimeBodyPart messagePart = new MimeBodyPart();
                multipart.addBodyPart(messagePart);  // adding message part

                //Setting the Email Encoding
                messagePart.setText(msg,"utf-8");
                messagePart.setHeader("Content-Type","text/plain; charset=\"utf-8\"");
                messagePart.setHeader("Content-Transfer-Encoding", "quoted-printable");
            }

            if (!StringUtil.isAllBlank(body)) {      
                MimeBodyPart bodyPart = new MimeBodyPart();
                multipart.addBodyPart(bodyPart);  // adding message part

                //Setting the Email Encoding
                bodyPart.setFileName(bodyFileName);
                bodyPart.setText(body,"utf-8");
                bodyPart.setHeader("Content-Type", bodyContentType);
                bodyPart.setHeader("Content-Transfer-Encoding", "quoted-printable");
            }

            message.setContent(multipart);
            Transport.send( message );

        } catch (Exception ex){
            System.out.println("sendEmail Exception:" + ex);
            ex.printStackTrace();
            throw new TException(ex);
        }
    }


    protected void setSession(Properties emailProp, boolean debug)
    {
        // create some properties and get the default Session
        session = Session.getDefaultInstance(emailProp, null);
        session.setDebug(debug);
    }

    public void setRecipientsBCC(ArrayList<String> recipientsBCC) {
        this.recipientsBCC = recipientsBCC;
    }
    
}


