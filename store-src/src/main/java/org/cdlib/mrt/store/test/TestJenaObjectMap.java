package org.cdlib.mrt.store.test;


import com.hp.hpl.jena.rdf.model.*;

import org.cdlib.mrt.utility.StringUtil;


public class TestJenaObjectMap
{

    protected static final String NAME = "TestJenaObjectMap";
    protected static final String MESSAGE = NAME + ": ";
    protected final static String NL = System.getProperty("line.separator");

    TestJenaObjectMap() { }

    /**
     * Main method
     */
    public static void main(String args[])
    {
        System.out.println("Begin " + NAME);
        try
        {

            TestJenaObjectMap test = new TestJenaObjectMap();
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
        String nie = "http://www.semanticdesktop.org/ontologies/2007/01/19/nie/";
        String rdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
        String rdfs = "http://www.w3.org/2001/01/rdf-schema#";

        String objectRef = "http://store.cdlib.org/object/15/ucsd%3A10326/1";

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

        // create predicates
        Property oreAggregates = ResourceFactory.createProperty(ore + "aggregates");
        Property nieMimeType = ResourceFactory.createProperty(nie + "mimeType");
        Property nieIdentifier = ResourceFactory.createProperty(nie + "identifier");

        // create tupples
        Resource object = model.createResource(objectRef);
        Resource [] objects = new Resource[aggregates.length];
        for (int i=0; i<aggregates.length; i++) {
            String aggregateS = aggregates[i];
            Resource aggregate = model.createResource(aggregateS);
            objects[i] = aggregate;
            model.add( object, oreAggregates, aggregate);
        }
        model.add(object, nieIdentifier, "ucsd:10326");
        model.add( objects[0], nieMimeType, "text/turtle" );
        model.add( objects[1], nieMimeType, "x/anvl" );
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