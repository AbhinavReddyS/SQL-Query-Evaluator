package ed.inf.adbs.lightdb.models;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import ed.inf.adbs.lightdb.util.ExpressionEvaluator;
import net.sf.jsqlparser.statement.select.OrderByElement;

public class SortOperator extends RelationalOperator {

	Boolean isProcessed;
	RelationalOperator child;	
	List<Tuple> lstTuples;
	List<OrderByElement> listOrderByElements;
	ExpressionEvaluator expEval = new ExpressionEvaluator();

	
	public SortOperator(RelationalOperator _child, List<OrderByElement> _listOrderByElements) {
		child = _child;
		listOrderByElements = _listOrderByElements;
		lstTuples = new ArrayList<Tuple>();
		isProcessed = false;
	}
	
	@Override
	public void reset() {
		child.reset();
		lstTuples = new ArrayList<Tuple>();
		isProcessed = true;
	}

	@Override
	public Tuple getNextTuple() {
		Tuple tuple = new Tuple();
		if(!isProcessed) {			
			while(!(tuple = child.getNextTuple()).isNull()) {
				if (!tuple.getValue().isEmpty())
					lstTuples.add(tuple);
			}
			isProcessed = true;
			lstTuples = expEval.evaluateSort(lstTuples, listOrderByElements);
		}
		if(lstTuples.size() == 0)
			return tuple;
		tuple = lstTuples.get(0);
		lstTuples.remove(0);
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
