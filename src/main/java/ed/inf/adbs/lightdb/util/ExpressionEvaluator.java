package ed.inf.adbs.lightdb.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Stack;
import java.util.stream.Collectors;

import ed.inf.adbs.lightdb.models.Tuple;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;

/**
 * Class to evaluate a record/tuple against a relational operator
 * @author abhinav
 *
 */
public class ExpressionEvaluator {

	/**
	 * Method to extract projected columns in the query from the tuple
	 * 
	 * @param lstSelectItems
	 * @param tuple
	 * @return
	 */
	public Tuple evaluateProject(List<SelectItem> lstSelectItems, Tuple tuple) {
		if (tuple.getValue().isEmpty())
			return tuple;
		ArrayList<String> lstColumns = new ArrayList<String>();
		SelectItemVisitor itemVisitor = new SelectItemVisitor() {

			@Override
			public void visit(AllColumns allColumns) {
				// TODO Auto-generated method stub

			}

			@Override
			public void visit(AllTableColumns allTableColumns) {
				// TODO Auto-generated method stub

			}

			@Override
			public void visit(SelectExpressionItem selectExpressionItem) {
				Column column = (Column) selectExpressionItem.getExpression();
				lstColumns.add(column.getTable().toString() + " " + column.getColumnName().toString());
			}
		};

		for (int i = 0; i < lstSelectItems.size(); i++) {
			SelectItem selectItem = lstSelectItems.get(i);
			selectItem.accept(itemVisitor);
		}

		tuple.setValue(lstColumns.stream().map(x -> tuple.get(tuple.columnIndex.get(x.split(" ")[0]).get(x.split(" ")[1]))).collect(Collectors.toList()));
		for(int i = 0; i < lstColumns.size(); i++) {
			String[] relCol = lstColumns.get(i).split(" ");
			tuple.columnIndex.get(relCol[0]).put(relCol[1], i);
		}
		return tuple;
	}

	/**
	 * Method to evaluate the selection conditions in the WHERE clause applicable to 
	 * the tuple being evaluated against the tuple.
	 * 
	 * @param expr
	 * @param tuple
	 * @return
	 * @throws JSQLParserException
	 */
	public boolean evaluateSelect(Expression expr, Tuple tuple) throws JSQLParserException {
		final Stack<Long> arthStack = new Stack<Long>();
		final Stack<Boolean> logicalStack = new Stack<Boolean>();
		final Stack<Column> columnStack = new Stack<Column>();
		ExpressionDeParser deparser = new ExpressionDeParser() {
			@Override
			public void visit(AndExpression andExpression) {
				super.visit(andExpression);
				Boolean const1 = logicalStack.pop();
				Boolean const2 = logicalStack.pop();
				logicalStack.push(const2.equals(true) && const1.equals(true));
			}

			@Override
			public void visit(EqualsTo equalsTo) {
				super.visit(equalsTo);
				if (arthStack.size() == 2) {
					Long const1 = arthStack.pop();
					Long const2 = arthStack.pop();
					logicalStack.push(const1 == const2);
				} else {
					logicalStack.push(true);
				}
			}

			@Override
			public void visit(GreaterThan greaterThan) {
				super.visit(greaterThan);
				if (arthStack.size() == 2) {
					Long const1 = arthStack.pop();
					Long const2 = arthStack.pop();
					logicalStack.push(const1 < const2);
				} else {
					logicalStack.push(true);
				}
			}

			@Override
			public void visit(GreaterThanEquals greaterThanEquals) {
				super.visit(greaterThanEquals);
				if (arthStack.size() == 2) {
					Long const1 = arthStack.pop();
					Long const2 = arthStack.pop();
					logicalStack.push(const1 <= const2);
				} else {
					logicalStack.push(true);
				}

			}

			@Override
			public void visit(MinorThan minorThan) {
				super.visit(minorThan);
				if (arthStack.size() == 2) {
					Long const1 = arthStack.pop();
					Long const2 = arthStack.pop();
					logicalStack.push(const1 > const2);
				} else {
					logicalStack.push(true);
				}

			}

			@Override
			public void visit(MinorThanEquals minorThanEquals) {
				super.visit(minorThanEquals);
				if (arthStack.size() == 2) {
					Long const1 = arthStack.pop();
					Long const2 = arthStack.pop();
					logicalStack.push(const1 >= const2);
				} else {
					logicalStack.push(true);
				}

			}

			@Override
			public void visit(NotEqualsTo notEqualsTo) {
				super.visit(notEqualsTo);
				if (arthStack.size() == 2) {
					Long const1 = arthStack.pop();
					Long const2 = arthStack.pop();
					logicalStack.push(const1 != const2);
				} else {
					logicalStack.push(true);
				}

			}

			@Override
			public void visit(LongValue longValue) {
				super.visit(longValue);
				if (columnStack.size() == 0 && arthStack.size() == 0) {
					arthStack.push(longValue.getValue());
				}else {
					if (columnStack.size() == 1) {
						Long value = getSelectColumnValue(tuple, columnStack.pop());
						if(value != null) {
							arthStack.push(value);
							arthStack.push(longValue.getValue());
						}
					}else {
						arthStack.push(longValue.getValue());	
					}
				}
			}

			@Override
			public void visit(Column tableColumn) {
				super.visit(tableColumn);
				if (columnStack.size() == 0 && arthStack.size() == 0) {
					columnStack.push(tableColumn);
				}else {
					if(arthStack.size() == 1) {
						Long value = getSelectColumnValue(tuple, tableColumn);
						if(value == null) {
							arthStack.pop();
						}else {
							arthStack.push(value);
						}
					}else {
						columnStack.pop();
					}
				}
			}
		};
		StringBuilder b = new StringBuilder();

		deparser.setBuffer(b);
		expr.accept(deparser);
		return logicalStack.pop();
	}

	/**
	 * Method to evaluate the join conditions in the WHERE clause applicable to the tuples and merge them, if the conditions
	 * are satisfied. If no condition is specified in the WHERE clause, the two tuples are merged and
	 * returned
	 * 
	 * @param expr
	 * @param lTuple
	 * @param rTuple
	 * @return
	 * @throws JSQLParserException
	 */
	public Tuple evaluateJoin(Expression expr, Tuple lTuple, Tuple rTuple) throws JSQLParserException {
		if (lTuple.getValue().isEmpty() || rTuple.getValue().isEmpty())
			return lTuple.getValue().isEmpty() ? lTuple : rTuple;
		
		Tuple tuple = new Tuple();		
		final Stack<Long> arthStack = new Stack<Long>();
		final Stack<Boolean> logicalStack = new Stack<Boolean>();
		final Stack<Column> columnStack = new Stack<Column>();
		
		ExpressionDeParser deparser = new ExpressionDeParser() {
			@Override
			public void visit(AndExpression andExpression) {
				super.visit(andExpression);
				Boolean const1 = logicalStack.pop();
				Boolean const2 = logicalStack.pop();
				logicalStack.push(const2.equals(true) && const1.equals(true));
			}

			@Override
			public void visit(EqualsTo equalsTo) {
				super.visit(equalsTo);
				if (columnStack.size() == 2) {
					Long const1 = getJoinColumnValue(lTuple, rTuple, columnStack.pop());
					Long const2 = getJoinColumnValue(lTuple, rTuple, columnStack.pop());
					if (const1 != null && const2 != null) {
						logicalStack.push(const1 == const2);
						return;
					}
				}
				logicalStack.push(true);
			}

			@Override
			public void visit(GreaterThan greaterThan) {
				super.visit(greaterThan);
				if (columnStack.size() == 2) {
					Long const1 = getJoinColumnValue(lTuple, rTuple, columnStack.pop());
					Long const2 = getJoinColumnValue(lTuple, rTuple, columnStack.pop());
					if (const1 != null && const2 != null) {
						logicalStack.push(const1 < const2);
						return;
					}
				}
				logicalStack.push(true);
			}

			@Override
			public void visit(GreaterThanEquals greaterThanEquals) {
				super.visit(greaterThanEquals);
				if (columnStack.size() == 2) {
					Long const1 = getJoinColumnValue(lTuple, rTuple, columnStack.pop());
					Long const2 = getJoinColumnValue(lTuple, rTuple, columnStack.pop());
					if (const1 != null && const2 != null) {
						logicalStack.push(const1 <= const2);
						return;
					}
				}
				logicalStack.push(true);
			}

			@Override
			public void visit(MinorThan minorThan) {
				super.visit(minorThan);
				if (columnStack.size() == 2) {
					Long const1 = getJoinColumnValue(lTuple, rTuple, columnStack.pop());
					Long const2 = getJoinColumnValue(lTuple, rTuple, columnStack.pop());
					if (const1 != null && const2 != null) {
						logicalStack.push(const1 > const2);
						return;
					}
				}
				logicalStack.push(true);
			}

			@Override
			public void visit(MinorThanEquals minorThanEquals) {
				super.visit(minorThanEquals);
				if (columnStack.size() == 2) {
					Long const1 = getJoinColumnValue(lTuple, rTuple, columnStack.pop());
					Long const2 = getJoinColumnValue(lTuple, rTuple, columnStack.pop());
					if (const1 != null && const2 != null) {
						logicalStack.push(const1 >= const2);
						return;
					}
				}
				logicalStack.push(true);
			}

			@Override
			public void visit(NotEqualsTo notEqualsTo) {
				super.visit(notEqualsTo);
				if (columnStack.size() == 2) {
					Long const1 = getJoinColumnValue(lTuple, rTuple, columnStack.pop());
					Long const2 = getJoinColumnValue(lTuple, rTuple, columnStack.pop());
					if (const1 != null && const2 != null) {
						logicalStack.push(const1 != const2);
						return;
					}
				}
				logicalStack.push(true);
			}

			@Override
			public void visit(LongValue longValue) {
				super.visit(longValue);
				if (columnStack.size() == 0 && arthStack.size() == 0) {
					arthStack.push(longValue.getValue());
				}else {
					if (columnStack.size() == 1) {
						columnStack.pop();
					}else {
						arthStack.pop();
					}
				}				
			}

			@Override
			public void visit(Column tableColumn) {
				super.visit(tableColumn);
				if (columnStack.size() == 0 && arthStack.size() == 0) {
					columnStack.push(tableColumn);
				}else {
					if (arthStack.size() == 1) {
						arthStack.pop();					
					}else {
						columnStack.push(tableColumn);
					}
				}
			}
		};
		
		if(expr != null) {
			StringBuilder b = new StringBuilder();
			deparser.setBuffer(b);
			expr.accept(deparser);
		}
		
		if(logicalStack.size() == 0 || (logicalStack.size() > 0 && logicalStack.pop())) {
			List<Long> tupleValue = lTuple.getValue().stream().map(x -> x).collect(Collectors.toList());
			tupleValue.addAll(rTuple.getValue());
			tuple.setValue(tupleValue);
			tuple.columnIndex = copy(lTuple.columnIndex);
			extend(tuple.columnIndex, rTuple.columnIndex);
			
			int i = lTuple.getValue().size();
			for(String relName : rTuple.columnIndex.keySet()) {
				for (String col : rTuple.columnIndex.get(relName).keySet()) {
					tuple.columnIndex.get(relName).put(col, i);
					i++;
				}	
			}
		}else {
			tuple.setValue(new ArrayList<Long>());
		}

		return tuple;
	}
	
	/**
	 * Method to sort a list of tuples given a sort order. If no order is specified, the ordering is done  
	 * in the order of values present in the tuple from left to right precedence.
	 * 
	 * @param lstTuple
	 * @param lstOrderByElement
	 * @return
	 */
	public List<Tuple> evaluateSort(List<Tuple> lstTuple, List<OrderByElement> lstOrderByElement){
		List<Column> lstColumns = new ArrayList<Column>();
		if(lstOrderByElement != null) {
			for(OrderByElement element : lstOrderByElement) {
				Column col = (Column)element.getExpression();
				lstColumns.add(col);
			}	
			Collections.sort(lstTuple, new Comparator<Tuple>(){
			    public int compare(Tuple t1, Tuple t2) {
			    	int out = 0;
			    	for(Column col : lstColumns) {
						out = t1.get(t1.columnIndex.get(col.getTable().toString()).get(col.getColumnName().toString()))
						.compareTo(t2.get(t2.columnIndex.get(col.getTable().toString()).get(col.getColumnName().toString())));
						if (out != 0)
							break;
					}
			        return out;
			    }
			});
		}else {
			Collections.sort(lstTuple, new Comparator<Tuple>(){
			    public int compare(Tuple t1, Tuple t2) {
			    	int out = 0;
			    	for(int i = 0 ; i < t1.getValue().size(); i++) {
						out = t1.get(i).compareTo(t2.get(i));
						if (out != 0)
							break;
					}
			        return out;
			    }
			});
		}
		

		return lstTuple;
	} 
	
	/**
	 * Given a column type element in the WHERE conditions of a join predicate, check if the column is present in 
	 * any of the left or right tuples.If yes, return the value corresponding to the column from the respective tuple. 
	 * 
	 * @param lTuple
	 * @param rTuple
	 * @param column
	 * @return
	 */
	private Long getJoinColumnValue(Tuple lTuple, Tuple rTuple, Column column) {
		String relationName = column.getTable().toString(); 
		Tuple tuple = lTuple.columnIndex.containsKey(relationName) ? lTuple : rTuple.columnIndex.containsKey(relationName) ? rTuple : null;
		return tuple == null ? null : tuple.get(tuple.columnIndex.get(relationName).get(column.getColumnName().toString()));
	}
	
	/**
	 * Given a column type element in the WHERE conditions of a select predicate, check if the column is present 
	 * in the tuple. If yes, return the value corresponding to the column from the tuple
	 * 
	 * 
	 * @param tuple
	 * @param column
	 * @return
	 */
	private Long getSelectColumnValue(Tuple tuple, Column column) {
		return tuple.columnIndex.containsKey(column.getTable().toString()) ? tuple.get(tuple.columnIndex.get(column.getTable().toString()).get(column.getColumnName().toString())) : null;
	}
	
	/**
	 * A custom deep copy method to copy variables of type HashMap<String, LinkedHashMap<String, Integer>>
	 * 
	 * @param columnIndex
	 * @return
	 */
	private HashMap<String, LinkedHashMap<String, Integer>> copy(HashMap<String, LinkedHashMap<String, Integer>> columnIndex) {
		HashMap<String, LinkedHashMap<String, Integer>> copy = new HashMap<String, LinkedHashMap<String, Integer>>();	
		for (String relName : columnIndex.keySet())
	    {
			LinkedHashMap<String, Integer> copyMap = new LinkedHashMap<String, Integer>();
			for (String colName : columnIndex.get(relName).keySet())
		    {
				copyMap.put(colName, columnIndex.get(relName).get(colName));
		    }
	        copy.put(relName, copyMap);
	    }
		return copy;
	}
	
	/**
	 * A custom extend method to extend variable of type HashMap<String, LinkedHashMap<String, Integer>>
	 * 
	 * @param baseIndex
	 * @param toAddIndex
	 */
	private void extend(HashMap<String, LinkedHashMap<String, Integer>> baseIndex, HashMap<String, LinkedHashMap<String, Integer>> toAddIndex) {
		HashMap<String, LinkedHashMap<String, Integer>> copy = new HashMap<String, LinkedHashMap<String, Integer>>();	
		for (String relName : toAddIndex.keySet())
	    {
			LinkedHashMap<String, Integer> copyMap = new LinkedHashMap<String, Integer>();
			for (String colName : toAddIndex.get(relName).keySet())
		    {
				copyMap.put(colName, toAddIndex.get(relName).get(colName));
		    }
			baseIndex.put(relName, copyMap);
	    }
	}
	

}
