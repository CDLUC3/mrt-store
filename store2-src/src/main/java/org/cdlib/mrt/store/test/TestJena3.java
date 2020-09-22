package org.cdlib.mrt.store.test;


import com.hp.hpl.jena.rdf.model.*;
import com.hp.hpl.jena.graph.Node;
import org.cdlib.mrt.utility.StringUtil;


public class TestJena3
{

    protected static final String NAME = "TestJena2";
    protected static final String MESSAGE = NAME + ": ";
    protected final static String NL = System.getProperty("line.separator");

    TestJena3() { }

    /**
     * Main method
     */
    public static void main(String args[])
    {
        System.out.println("Begin " + NAME);
        try
        {

            TestJena3 test = new TestJena3();
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
        String obj = "http://merritt.cdlib.org/object/";
        String mrt = "http://merritt.cdlib.org/terms/";
        String nie = "http://www.semanticdesktop.org/ontologies/2007/01/19/nie/";
        String rdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
        String rdfs = "http://www.w3.org/2001/01/rdf-schema#";

        String objectRef = "http://store.cdlib.org/object/15/ucsd%3A10326/1";
        String objectBase = "http://store.cdlib.org/content/15";

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
        model.setNsPrefix("obj", obj);
/*

    <nodeState>http://localhost:28080/storage/state/15</nodeState>
    <numFiles>87</numFiles>
    <numVersions>5</numVersions>
    <statsSize>1156188</statsSize>
    <statsNumFiles>51</statsNumFiles>

    <currentVersionState>http://localhost:28080/storage/state/15/12345-abcde/5</currentVersionState>
    <versionStates>
        <versionState>http://localhost:28080/storage/state/15/12345-abcde/1</versionState>
        <versionState>http://localhost:28080/storage/state/15/12345-abcde/2</versionState>
        <versionState>http://localhost:28080/storage/state/15/12345-abcde/3</versionState>
        <versionState>http://localhost:28080/storage/state/15/12345-abcde/4</versionState>

        <versionState>http://localhost:28080/storage/state/15/12345-abcde/5</versionState>
    </versionStates>
    <object>http://localhost:28080/storage/content/15/12345-abcde</object>
    <size>2841088</size>
    <identifier>12345-abcde</identifier>
 */
        // create predicates
        Property nodeState = ResourceFactory.createProperty(obj + "nodeState");
        Property numFiles = ResourceFactory.createProperty(obj + "numFiles");
        Property numVersions = ResourceFactory.createProperty(obj + "numVersions");
        Property statsSize = ResourceFactory.createProperty(obj + "statsSize");
        Property statsNumFiles = ResourceFactory.createProperty(obj + "statsNumFiles");
        Property currentVersionState = ResourceFactory.createProperty(obj + "currentVersionState");

        Property versionStates = ResourceFactory.createProperty(obj + "versionStates");
        Property versionState = ResourceFactory.createProperty(obj + "versionState");

        // create tupples
        Resource object = model.createResource(objectRef);
        Resource id = model.createResource(rdf + "ID");
        Property rdfId = ResourceFactory.createProperty(rdf + "id");

        object.addLiteral(rdfId, 15);
        object.addProperty(nodeState, "http://localhost:28080/storage/state/15");
        object.addLiteral(numFiles, 87);
        object.addLiteral(numVersions, 5);
        object.addLiteral(statsSize, 1156188);
        object.addLiteral(statsNumFiles, 51);
        object.addProperty(versionState, "http://localhost:28080/storage/state/15/12345-abcde/1");
        object.addProperty(versionState, "http://localhost:28080/storage/state/15/12345-abcde/2");
        object.addProperty(versionState, "http://localhost:28080/storage/state/15/12345-abcde/3");
        object.addProperty(versionState, "http://localhost:28080/storage/state/15/12345-abcde/4");
 


        /*
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
         * */
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