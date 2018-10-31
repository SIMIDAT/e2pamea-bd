package main;

import org.apache.commons.cli.*;

/**
 * Class for parsing the parameters of each algorithm
 */
public class ParametersParser {

    Options options;
    CommandLineParser parser;
    HelpFormatter formatter ;


    public ParametersParser(){

        options = new Options();
        parser = new DefaultParser();
        formatter = new HelpFormatter();

        /*
        The addOption method has three parameters.
        The first parameter is a java.lang.String that represents the option.
        The second parameter is a boolean that specifies whether the option requires an argument or not.
        In the case of a boolean option (sometimes referred to as a flag) an argument value is not present so false is passed.
        The third parameter is the description of the option. This description will be used in the usage text of the application.
         */
       // options.addOption("t", false, "Description of the method");
        Option t = OptionBuilder.hasArgs(2).withArgName("hola").withLongOpt("tiempo").withDescription("La descripcion").create("t");
        options.addOption(t);

        options.addOption("c", false, "Description of the method");

        // Automatically generate the help statement
        //formatter.printHelp( "ant", options );
        formatter.printHelp("ant", "USA ESTO CUANDO QUIERAS", options, "ESTO ES EL FOOTER");
    }
}
