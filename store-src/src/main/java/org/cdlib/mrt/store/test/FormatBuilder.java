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

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.PrintStream;

import org.cdlib.mrt.formatter.FormatterInf;
import org.cdlib.mrt.formatter.FormatterAbs;
import org.cdlib.mrt.utility.TFileLogger;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StateInf;
import org.cdlib.mrt.utility.StringUtil;


public class FormatBuilder
{


    protected static final String NAME = "FormatBuilder";
    protected static final String MESSAGE = NAME + ": ";
    protected LoggerInf logger = null;

    protected FormatBuilder(LoggerInf logger)
    {
        this.logger = logger;
    }

    public static FormatBuilder getFormatBuilder(LoggerInf logger)
    {
        return new FormatBuilder(logger);
    }

    public static FormatBuilder getFormatBuilder()
    {
        LoggerInf logger = new TFileLogger(NAME, 10, 10);
        return new FormatBuilder(logger);
    }

    public void formatState(
            FormatterInf.Format format,
            StateInf state,
            File outFile)
    {
        try {
            log(MESSAGE + "formatState - outFile=" + outFile.getCanonicalPath(), 0);
            FileOutputStream outFileStream = new FileOutputStream(outFile);
            PrintStream printStream = new PrintStream(outFileStream, true, "utf-8");
            FormatterInf formatter = FormatterAbs.getFormatter(format, logger);
            formatter.format(state, printStream);
            printStream.close();

        } catch (Exception ex) {
            log(MESSAGE + "Exception:" + ex, 0);
            log(MESSAGE + "Trace:" + StringUtil.stackTrace(ex), 0);
        }
    }

    public void printIt(String header, File inFile)
            throws Exception
    {
        System.out.println(header);
        FileInputStream fis = new FileInputStream(inFile);
        BufferedReader br = new BufferedReader(new InputStreamReader(fis, "utf-8"));
        String line = null;
        while (true) {
            line = br.readLine();
            if (line == null) break;
            System.out.println(line);
        }
    }


    protected void log(String msg, int lvl)
    {
        logger.logMessage(msg, 0, true);
    }

}
