package ed.inf.adbs.lightdb;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.io.File;
import ed.inf.adbs.lightdb.models.DuplicateEliminationOperator;
import ed.inf.adbs.lightdb.models.JoinOperator;
import ed.inf.adbs.lightdb.models.ProjectOperator;
import ed.inf.adbs.lightdb.models.RelationalOperator;
import ed.inf.adbs.lightdb.models.ScanOperator;
import ed.inf.adbs.lightdb.models.SelectOperator;
import ed.inf.adbs.lightdb.models.SortOperator;
import ed.inf.adbs.lightdb.util.ExpressionEvaluator;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.operators.relational.ComparisonOperator;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;

/**
 * Class to build the query plan tree and invoke its evaluation against a query.
 * 
 * @author abhin
 *
 */
public class QueryBuilder extends ExpressionVisitorAdapter {

	/**
	 * Root node of the query plan tree;
	 */
	RelationalOperator root;

	/**
	 * Evaluates the query against the query plan. Invokes the dump method for the query plan tree's root node.
	 * 
	 * @param outputFile
	 */
	public void evaluate(String outputFile) {
		root.dump(new File(outputFile));
	}
	
	/**
	 * Constructs a left deep join query plan tree. 
	 * Build the tree of relational operators from leaf to root:
	 * scan -> selection -> join -> projection -> order by -> distinct
	 * 
	 * 
	 * @param plainSelect
	 * @throws Exception
	 */
	public void buildQPlanTree(PlainSelect plainSelect) throws Exception {
		Expression expr = plainSelect.getWhere();	
		LinkedHashMap <String, RelationalOperator> relationalOps = new LinkedHashMap<String, RelationalOperator>();
		
		/**
		 * Identifies all the scan nodes based on the relations present in the from/join items
		 */
		buildScanNodes(plainSelect, relationalOps);
		
		/**
		 * For each of the scan nodes if the nodes are present in the where clause, assign 
		 * a parent select node to that scan node respectively.
		 */
		if (expr != null) {
			buildSelectNodes(plainSelect, expr, relationalOps);
		}
		
		/**
		 * Joins the appropriate scan/selection nodes if more than one scan/select node is present.
		 * In a left to right precedence of the from/join items.
		 * Further, joins the obtained join nodes iteratively until a single join node is obtained.
		 */
		int index = 0;
		for(String key : relationalOps.keySet()) {
			if (index != 0) {
				root = new JoinOperator(root, relationalOps.get(key), expr);
			}else {
				root = relationalOps.get(key);
			}
			index++;
		}
		
		//Assign the root node to be child node of a Projection node (becoming the new root)
		if(!plainSelect.getSelectItems().get(0).toString().equals("*"))
			root = new ProjectOperator(root, plainSelect.getSelectItems());
		
		//Assign the root node to be child node of a Sort node (becoming the new root)
		if(plainSelect.getOrderByElements() != null || plainSelect.getDistinct() != null)
			root = new SortOperator(root, plainSelect.getOrderByElements());
		
		//Assign the root node to be child node of a Dupliciate Elimination node (becoming the new root)
		if(plainSelect.getDistinct() != null)
			root = new DuplicateEliminationOperator(root);
	}

	
	/**
	 * Identifies the scan nodes from the query.
	 * 
	 * @param plainSelect
	 * @param relationalOps
	 * @throws FileNotFoundException
	 */
	private void buildScanNodes(PlainSelect plainSelect, LinkedHashMap<String, RelationalOperator> relationalOps)
			throws FileNotFoundException {
		boolean hasAlias = plainSelect.getFromItem().getAlias() != null;
		
		String fromRelation = hasAlias ? plainSelect.getFromItem().toString().split(" ")[0] : plainSelect.getFromItem().toString();
		String alias = hasAlias ? plainSelect.getFromItem().toString().split(" ")[1].trim() : null; 
		relationalOps.put(hasAlias ? alias : fromRelation, new ScanOperator(fromRelation, alias));
		
		if(plainSelect.getJoins() != null) {
			for(Join relName : plainSelect.getJoins()) {
				String joinRelation = hasAlias ? relName.toString().split(" ")[0] : relName.toString();
				alias = hasAlias ? relName.toString().split(" ")[1].trim() : null;
				relationalOps.put(hasAlias ? alias : joinRelation, new ScanOperator(joinRelation, alias));
			}			
		}
	}

	/**
	 * Identifies all the selection nodes from the scan nodes discovered.
	 * 
	 * @param plainSelect
	 * @param expr
	 * @param relationalOps
	 */
	private void buildSelectNodes(PlainSelect plainSelect, Expression expr,
		LinkedHashMap<String, RelationalOperator> relationalOps) {
		List<String> visited = new ArrayList<String>();
		ExpressionVisitorAdapter visitorAdapter = new ExpressionVisitorAdapter() {
			@Override
			protected void visitBinaryExpression(BinaryExpression expr) {
				if (expr instanceof ComparisonOperator) {
					Expression leftExp = expr.getLeftExpression();
					Expression rightExp = expr.getRightExpression();
					if (((leftExp instanceof Column) && !(rightExp instanceof Column))
							|| (!(leftExp instanceof Column) && (rightExp instanceof Column))) {
						Column col = (Column) (leftExp instanceof Column ? leftExp : rightExp);
						String key = col.getTable().toString();
						if (relationalOps.containsKey(key) && !visited.contains(key)) {
							RelationalOperator select = new SelectOperator(relationalOps.get(key),
									plainSelect.getWhere());
							relationalOps.put(key, select);
							visited.add(key);
						}
					}
				}
				super.visitBinaryExpression(expr);
			}
		};
		expr.accept(visitorAdapter);
		if(visited.size() == 0) {			
			Map.Entry<String,RelationalOperator> entry = relationalOps.entrySet().iterator().next();
			relationalOps.put(entry.getKey(), new SelectOperator(relationalOps.get(entry.getKey()),
									plainSelect.getWhere()));
		}
	}

}
