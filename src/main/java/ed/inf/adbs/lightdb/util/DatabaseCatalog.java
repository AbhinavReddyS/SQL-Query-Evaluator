package ed.inf.adbs.lightdb.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Scanner;

/**
 * Class to store information about and interact with, the physical database schema.
 * 
 * @author abhin
 *
 */
public class DatabaseCatalog {
	   /**
	    * Invokes the private constructor of DatabaseCatalog
	    */
	   private static DatabaseCatalog dbInstance = new DatabaseCatalog();
	   private static File dbDir;
	   public static HashMap<String, LinkedHashMap<String, Integer>> dbSchema = new HashMap<String, LinkedHashMap<String, Integer>>();
	   
	   /**
	    * Making the constructor private so that this class cannot be instantiated
	    */
	   private DatabaseCatalog(){}

	   /**
	    * Returns the only instance of the class available.
	    * 
	    * @param databaseDir
	    * @return dbInstance
	    */
	   public static DatabaseCatalog getDBInstance(String databaseDir){
		   dbDir = new File(databaseDir);
		   File schema = new File(dbDir.getAbsoluteFile() + File.separator + "schema.txt");
		   createDBSchema(schema);
		   return dbInstance;
	   }
	   
	   /**
	    * Constructs and stores the db schema into a LinkedHashMap,
	    * in order to preserve the order of columns
	    * 
	    * @param schema
	    */
	   private static void createDBSchema(File schema) {
		    try {
		        Scanner myReader = new Scanner(schema);
		        while (myReader.hasNextLine()) {
		          List<String> data = Arrays.asList(myReader.nextLine().split(" "));
		          LinkedHashMap<String, Integer> relation = new LinkedHashMap<String, Integer>();
		          for (int i = 1; i < data.size(); i++) {
		        	  relation.put(data.get(i), i-1);
		          }
		          //HashMap{relationName : {columnName : index}}
		          dbSchema.put(data.get(0), relation);
		        }
		        myReader.close();
		      } catch (FileNotFoundException e) {
		        System.out.println("An error occurred.");
		        e.printStackTrace();
		      }
	   }
	   
	   /**
	    * Returns the file of a relation, given the relation name
	    * 
	    * @param relation
	    * @return A filepath string for the relation.
	    */
	   public static String getRelationTuples(String relation) {
		   return dbDir.getAbsolutePath() + File.separator + "data" + File.separator + relation + ".csv";
	   }
}
