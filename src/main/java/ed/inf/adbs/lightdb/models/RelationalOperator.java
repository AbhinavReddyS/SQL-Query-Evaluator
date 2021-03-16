package ed.inf.adbs.lightdb.models;

import java.io.File;

public abstract class RelationalOperator {
	public abstract void reset();
	public abstract Tuple getNextTuple();
	public abstract void dump(File output);
}
