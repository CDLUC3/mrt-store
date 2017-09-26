package org.cdlib.mrt.store.test;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.Reader;
import org.cdlib.mrt.utility.TException;
import com.hp.hpl.jena.rdf.model.*;
import com.hp.hpl.jena.graph.Node;
import org.cdlib.mrt.utility.StringUtil;


public class TestJena4
{

    protected static final String NAME = "TestJena2";
    protected static final String MESSAGE = NAME + ": ";
    protected final static String NL = System.getProperty("line.separator");

    TestJena4() { }

    /**
     * Main method
     */
    public static void main(String args[])
    {
        System.out.println("Begin " + NAME);
        try
        {

            TestJena4 test = new TestJena4();
            test.run();

        }  catch(Exception e)  {
                System.out.println(
                    "Main: Encountered exception:" + e);
                System.out.println(
                        StringUtil.stackTrace(e));
        }
    }

    protected void run()
    {
        try {
            /*
            Model model = buildModel();
            dumpModel(model);
            print(model);
             *
             */
            //test1("testresources/ontology-version.rdf");

            //test1("testresources/dc-example.rdf");
            //test2("testresources/ontology-version.rdf");
            //test2("testresources/dc-example.rdf");
            test2("testresources/ontology-example2.rdf");

        }  catch(Exception e)  {
                log("Main: Encountered exception:" + e, 0);
                log(StringUtil.stackTrace(e), 0);
        }
    }

    protected void test1(String path)
    {
        System.out.println("***** TEST1:" + path);
        try {
            Model model = buildModel(path);
            dumpModel(model);
            print(model);

        }  catch(Exception e)  {
                log("Main: Encountered exception:" + e, 0);
                log(StringUtil.stackTrace(e), 0);
        }
    }

    public static void test2(String path)
        throws TException
    {

        System.out.println("***** TEST2:" + path);
        Model model = ModelFactory.createDefaultModel();
        try {

            InputStream inStream = ClassLoader.getSystemResourceAsStream(path);

            RDFReader reader = model.getReader();
            reader.read(model, inStream, "");

            System.out.println("RDF/XML******************************************");
            model.write(System.out, "RDF/XML");
            System.out.println("RDF/XML-ABBREV******************************************");
            model.write(System.out, "RDF/XML-ABBREV");
            System.out.println("TURTLE******************************************");
            model.write(System.out, "TURTLE");

        } catch (Exception ex) {
            throw new TException(ex);
        }
    }

    public static Model buildModel(String path)
        throws TException
    {
        Model model = ModelFactory.createDefaultModel();
        try {

            InputStream inStream = ClassLoader.getSystemResourceAsStream(path);

            RDFReader reader = model.getReader();
            reader.read(model, inStream, "");

        } catch (Exception ex) {
            throw new TException(ex);
        }

        return model;
    }

    public static void dumpModel(Model model)
    {

        System.out.println(NL + "**** dumpModel ****");
        
        // list the statements in the graph
        StmtIterator iter = model.listStatements();

        // print out the predicate, subject and object of each statement
        while (iter.hasNext()) {
            Statement stmt      = iter.nextStatement();         // get next statement
            Resource  subject   = stmt.getSubject();   // get the subject
            Property  predicate = stmt.getPredicate(); // get the predicate
            RDFNode   object    = stmt.getObject();    // get the object

            System.out.print(subject.toString());
            System.out.print(" " + predicate.toString() + " ");
            if (object instanceof Resource) {
                System.out.print(object.toString());
            } else {
                // object is a literal
                System.out.print(" \"" + object.toString() + "\"");
            }
            System.out.println(" .");
        }
    }

    public static void print(Model model)
    {
        String [] formats = {
            "RDF/XML",
            "RDF/XML-ABBREV",
            "N-TRIPLE",
            "TURTLE",
            "TTL",
            "N3"};
        for (int i=0; i<formats.length; i++) {
            String format = formats[i];

            System.out.println("");
            System.out.println("**** " + format + " ****");
            model.write(System.out, format);
        }
    }


    protected static void log(String msg, int lvl)
    {
        System.out.println(msg);
        //logger.logMessage(msg, 0, true);
    }

}