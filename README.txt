Query Plan Tree Building:

1) Identify all relations in the from and join items and build scan nodes out of them.
2) For each of the scan nodes if the corresponding relation is present in the where clause
create a select node and assign the scan node as its child respectively.
3) Now we have  a set of scan and select nodes. Create a join node with its left and right child
as the nodes from the set of scan/select nodes in a left to rigth order of the from clause.
Iteratively perform join on the set elements and the join nodes formed by combining these elements
until we are left with only one root join node(whose child nodes can be scan/select/join nodes).
4) Assign the root node to be child node of a Projection node (becoming the new root), if applicable.
5) Assign the root node to be child node of a Sort node (becoming the new root), if applicable
6) Assign the root node to be child node of a Dupliciate Elimination node (becoming the new root), if applicable.


Query Plan Evaluation:

1) For every node check the type of the node and invoke appropriate evaluation method.
2) The evaluation method checks the relation(s) present in the node and picks the respective 
conditions from the WHERE clause and processes them or does a cross product if necessary.
3) The result obtained is pushed to its parent node or as output.

**Please find inline and method level comments for further understanding.