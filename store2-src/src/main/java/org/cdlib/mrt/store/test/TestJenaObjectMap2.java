package org.cdlib.mrt.store.test;


import com.hp.hpl.jena.rdf.model.*;

import org.cdlib.mrt.utility.StringUtil;


public class TestJenaObjectMap2
{

    protected static final String NAME = "TestJenaObjectMap";
    protected static final String MESSAGE = NAME + ": ";
    protected final static String NL = System.getProperty("line.separator");

    TestJenaObjectMap2() { }

    /**
     * Main method
     */
    public static void main(String args[])
    {
        System.out.println("Begin " + NAME);
        try
        {

            TestJenaObjectMap2 test = new TestJenaObjectMap2();
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
            Model model = buildModel();
            dumpModel(model);
            print(model);

        }  catch(Exception e)  {
                log("Main: Encountered exception:" + e, 0);
                log(StringUtil.stackTrace(e), 0);
        }
    }

    public static Model buildModel()
    {

        // resource URLs
        String ore = "http://www.openarchives.org/ore/terms/";
        String mrt = "http://uc3.cdlib.org/ontology/mrt/mom#";
        String nie = "http://www.semanticdesktop.org/ontologies/2007/01/19/nie/";
        String rdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
        String rdfs = "http://www.w3.org/2001/01/rdf-schema#";

        String objectIngestRef = "http://linux-oj8o.ad.ucop.edu:8080/storage/state/15/13030-xxxxxxxx";
        String objectStorageRef = "http://linux-oj8o.ad.ucop.edu:8080/storage/state/15/13030-xxxxxxxx/0/mrt-ingest.txt";
        String metadataIngest = "http://linux-oj8o.ad.ucop.edu:8080/storage/state/15/13030-xxxxxxxx/0/mrt-ingest.txt";

        String [] aggregates = {
            "http://store.cdlib.org/version/15/ucsd%3A10326/1/system%2Fmrt-object-map.ttl",
            "http://store.cdlib.org/version/15/ucsd%3A10326/1/system%2Fmrt-splash.txt",
            "http://store.cdlib.org/file/15/ucsd%3A10326/1/producer%2F10326.mrt-splash.txt",
            "http://store.cdlib.org/file/15/ucsd%3A10326/1/producer%2FWu_ucsd_0033D_10326-marc.txt",
            "http://store.cdlib.org/file/15/ucsd%3A10326/1/producer%2FWu_ucsd_0033D_10326.pdf",
            "http://store.cdlib.org/file/15/ucsd%3A10326/1/producer%2FWu_ucsd_0033D_10326_DATA.xml"};

        // initialize
        Model model = ModelFactory.createDefaultModel();

        // set prefix associations used in Turtle display
        model.setNsPrefix("ore", ore);
        model.setNsPrefix("nie", nie);
        model.setNsPrefix("mrt", mrt);

        // create predicates
        Property oreAggregates = ResourceFactory.createProperty(ore + "aggregates");
        Property rdfType = ResourceFactory.createProperty(rdf + "type");
        Property seeAlso = ResourceFactory.createProperty(rdf + "seeAlso");
        Property hasMimeType = ResourceFactory.createProperty(mrt + "has-MIME-type");
        Property ingestMetadataP = ResourceFactory.createProperty(mrt + "ingest-metadata");

        // create tupples
        Resource objectIngestRes = model.createResource(objectIngestRef);
        Resource objectStorageRes = model.createResource(objectStorageRef);
        Resource ingestMetadataR = model.createResource(mrt + "ingest-metadata");
        
        model.add(objectIngestRes, seeAlso, objectStorageRes);
        model.add( objectStorageRes, rdfType, ingestMetadataR );
        //model.add( objectStorageRes, rdfType, "dog" );
        model.add( objectStorageRes, hasMimeType, "text/anvl" );
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