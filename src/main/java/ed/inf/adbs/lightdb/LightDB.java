package ed.inf.adbs.lightdb;

import java.io.File;
import java.io.StringReader;
import java.util.Scanner;

import ed.inf.adbs.lightdb.util.DatabaseCatalog;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;

/**
 * Lightweight in-memory database system
 *
 */
public class LightDB {

	public static void main(String[] args) {

		if (args.length != 3) {
			System.err.println("Usage: LightDB database_dir input_file output_file");
			return;
		}

		String databaseDir = args[0];
		String inputFile = args[1];
		String outputFile = args[2];
		DatabaseCatalog.getDBInstance(databaseDir);
		parse(inputFile, outputFile);
	}

	/**
	 * Initial driver method. Reads the input file, invokes the method to build query plan, invokes 
	 * the method to evaluate output the result.
	 * @param inputFile
	 * @param outputFile
	 */
	public static void parse(String inputFile, String outputFile) {
		CCJSqlParserManager sqlParser = new CCJSqlParserManager();
		QueryBuilder queryBuilder = new QueryBuilder();
		try {
			Scanner sc = new Scanner(new File(inputFile));
			Statement statement = sqlParser.parse(new StringReader(sc.nextLine()));
			sc.close();
//			Statement statement = sqlParser.parse(new StringReader("SELECT * FROM Sailors S1, Sailors S2 where S1.A = 1")); 
			if (statement != null) {
				System.out.println("Read statement: " + statement);
				Select select = (Select) statement;
				PlainSelect plainSelect = (PlainSelect) select.getSelectBody();	
				queryBuilder.buildQPlanTree(plainSelect);
				queryBuilder.evaluate(outputFile);
				System.out.println("Done");
			}
		} catch (Exception e) {
			System.err.println("Error in the query entered");
			e.printStackTrace();
		}
	}
	
}
