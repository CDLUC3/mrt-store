/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.cdlib.mrt.store.test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import org.cdlib.mrt.core.FileComponent;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.StringUtil;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.TFileLogger;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.store.tools.FileFromUrl;
import org.cdlib.mrt.utility.ArchiveBuilderBase;
/**
 *
 * @author replic
 */
public class TestArchiveBuilderBase 
{

    protected static final String NAME = "TestCRC";
    protected static final String MESSAGE = NAME + ": ";
    private LoggerInf logger = null;
    
    
    public static void main(String[] args) 
            
            throws Exception 
    {
        System.out.println("MinioTest2");
        String key = "ark:/28722/k23j3911v|1|system/mrt-mom.txt";
        
        LoggerInf logger = new TFileLogger(NAME, 50, 50);
        File testFile = new File("/apps/replic/tasks/storage/220906-zip-mod/bb9967011v/ark+=20775=bb9967011v");
        File toArchive = new File("/apps/replic/tasks/storage/220906-zip-mod/bb9967011v/test/comp.zip");;
        //File testFile = new File("/apps/replic/tasks/storage/220906-zip-mod/fk4jh5zg9/ark+=99999=fk4jh5zg9");
        //File toArchive = new File("/apps/replic/tasks/storage/220906-zip-mod/fk4jh5zg9/test/comp.zip");
        ArchiveBuilderBase base 
                = ArchiveBuilderBase.getArchiveBuilderBase(testFile, toArchive, logger, 
                        ArchiveBuilderBase.ArchiveType.zip).setDeleteFileAfterCopy(true);
        base.buildArchive(true);
    }
    
}
