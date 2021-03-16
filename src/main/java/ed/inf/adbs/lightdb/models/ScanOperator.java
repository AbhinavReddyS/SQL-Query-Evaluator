package ed.inf.adbs.lightdb.models;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Scanner;
import java.util.stream.Collectors;

import ed.inf.adbs.lightdb.util.DatabaseCatalog;

public class ScanOperator extends RelationalOperator {

	private File relationFile;
	private String relationName;
	private String alias;
	private LinkedHashMap<String, Integer> relationColumns;
	Scanner sc;
	
	public ScanOperator(String relationName, String alias) throws FileNotFoundException {
		relationFile = new File(DatabaseCatalog.getRelationTuples(relationName));
		this.relationName = relationName;
		relationColumns = new LinkedHashMap<String, Integer>();		
		relationColumns = DatabaseCatalog.dbSchema.get(relationName);
		sc = new Scanner(relationFile);
		this.alias = alias; 
	}
	
	@Override
	public void reset() {
		sc.close();
		try {
			sc = new Scanner(relationFile);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	@Override
	public Tuple getNextTuple() {
		Tuple tuple = new Tuple();
		if(sc.hasNext())
		{
			tuple.setValue(Arrays.stream(sc.next().split(","))
						.map(Long::parseLong).collect(Collectors.toList())); // find and returns the next complete token from this scanner
			tuple.columnIndex.put(alias == null ? relationName : alias, relationColumns);
		}else {
			sc.close();
		}
		return tuple;
	}

//	@Override
//	public void dump(File output) {
//		Tuple tuple;
//		while(!(tuple = getNextTuple()).isNull()) {
//			System.out.println(tuple.getValue());
//		}
//	}
	
	@Override
	public void dump(File output) {
		Tuple tuple;
		PrintWriter pw = null;
		try {
			if (!output.exists())
				output.createNewFile();
			pw = new PrintWriter(output);
			while (!(tuple = getNextTuple()).isNull()) {
				StringBuilder strbul = new StringBuilder();
				Iterator<Long> iter = tuple.getValue().iterator();
				while (iter.hasNext()) {
					strbul.append(iter.next());
					if (iter.hasNext()) {
						strbul.append(",");
					}
				}
				pw.write(strbul.toString());
				pw.write("\n");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		pw.close();
	}
	 
}
