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
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectItem;

public class JoinOperator extends RelationalOperator {

	RelationalOperator lChild;
	RelationalOperator rChild;
	Expression expression;
	Tuple lChildTuple;
	ExpressionEvaluator expEval = new ExpressionEvaluator();

	public JoinOperator(RelationalOperator _lchild, RelationalOperator _rchild, Expression _expression) {
		this.lChild = _lchild;
		this.rChild = _rchild;
		this.expression = _expression;
		lChildTuple = new Tuple();

	}

	@Override
	public void reset() {
		lChild.reset();
		rChild.reset();
		lChildTuple = new Tuple();
	}

	@Override
	public Tuple getNextTuple() {
		Tuple rChildTuple = new Tuple();
		Tuple tuple = new Tuple();


		if (lChildTuple.isNull())
			lChildTuple = lChild.getNextTuple();

		if (!lChildTuple.isNull()) {
			rChildTuple = rChild.getNextTuple();
			if (!rChildTuple.isNull()) {
				try {
					tuple = expEval.evaluateJoin(expression, lChildTuple, rChildTuple);
				} catch (Exception e) {
					e.printStackTrace();
				}
			} else {
				rChild.reset();
				lChildTuple = new Tuple();
				tuple.setValue(new ArrayList<Long>());
			}
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
