package ed.inf.adbs.lightdb.models;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.List;

import ed.inf.adbs.lightdb.util.ExpressionEvaluator;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.SelectItem;

public class ProjectOperator extends RelationalOperator {
	RelationalOperator child;
	List<SelectItem> lstSelectItems;	
	ExpressionEvaluator expEval = new ExpressionEvaluator();
	
	public ProjectOperator(RelationalOperator _child, List<SelectItem> _lstSelectItems) {
		child = _child;
		lstSelectItems = _lstSelectItems;
	}
	
	@Override
	public void reset() {
		child.reset();
	}

	@Override
	public Tuple getNextTuple() {
		Tuple tuple = new Tuple();	
		tuple = child.getNextTuple();
		if(!tuple.isNull()) {
			tuple = expEval.evaluateProject(lstSelectItems, tuple);
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
