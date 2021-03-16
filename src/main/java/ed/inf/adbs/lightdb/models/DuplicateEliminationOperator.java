package ed.inf.adbs.lightdb.models;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import ed.inf.adbs.lightdb.util.ExpressionEvaluator;
import net.sf.jsqlparser.statement.select.Distinct;
import net.sf.jsqlparser.statement.select.OrderByElement;

public class DuplicateEliminationOperator extends RelationalOperator{
	
	RelationalOperator child;	
	ExpressionEvaluator expEval = new ExpressionEvaluator();
	Tuple prev = new Tuple();
	
	public DuplicateEliminationOperator(RelationalOperator _child) {
		child = _child;
	}
	
	@Override
	public void reset() {
		child.reset();
	}

	@Override
	public Tuple getNextTuple() {
		Tuple tuple = new Tuple();
		if (!(tuple = child.getNextTuple()).isNull()) {
			if (!tuple.getValue().isEmpty() && (prev.isNull() || !tuple.getValue().equals(prev.getValue()))) {
				prev = tuple;
				return tuple;
			}
			tuple.setValue(new ArrayList<Long>());
		}
		return tuple;
	}

//	@Override
//	public void dump(File output) {
//		Tuple tuple;
//		while(!(tuple = getNextTuple()).isNull()) {
//			if (!tuple.getValue().isEmpty())
//				System.out.println(tuple.getValue());
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
				if (!tuple.getValue().isEmpty()) {
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
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		pw.close();
	}
	 

}
