package ed.inf.adbs.lightdb.models;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import ed.inf.adbs.lightdb.util.ExpressionEvaluator;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;

public class SelectOperator extends RelationalOperator{
	RelationalOperator child;
	Expression expression;	
	ExpressionEvaluator expEval = new ExpressionEvaluator();
	
	public SelectOperator(RelationalOperator _child, Expression _expression) {
		child = _child;
		expression = _expression;
	}
	
	@Override
	public void reset() {
		child.reset();
	}

	@Override
	public Tuple getNextTuple() {
		Tuple tuple = new Tuple();
		tuple = child.getNextTuple();
		try {
			if (!tuple.isNull() && !expEval.evaluateSelect(expression, tuple)) {
				tuple.clear();
			}
		} catch (JSQLParserException e) {
			e.printStackTrace();
		}	
		return tuple;
	}

//	@Override
//	public void dump(File output) {
//		Tuple tuple;
//		while (!(tuple = getNextTuple()).isNull()) {
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
