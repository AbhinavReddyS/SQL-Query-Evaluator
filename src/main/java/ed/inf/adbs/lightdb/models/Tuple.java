package ed.inf.adbs.lightdb.models;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;

public class Tuple {
	private List<Long> value;
	public HashMap<String, LinkedHashMap<String, Integer>> columnIndex;
	
	public Tuple() {
		value = null;
		columnIndex = new HashMap<String, LinkedHashMap<String, Integer>>();
	}
	
	public List<Long> getValue() {
		return value;
	}
	public void setValue(List<Long> value) {
		this.value = new ArrayList<Long>();
		this.value = value;
	}
	public void clear() {
		if(this.value != null) {
			this.value.clear();
		}
	}	
	public Long get(int index) {
		return value.get(index);
	}
	
	public void add(Long val) {
		value.add(val);
	}
	
	public void set(int index, Long val) {
		value.set(index, val);
	}
	
	public boolean isNull() {
		if (this.value == null)
			return true;
		else
			return false;
	
	}

}
