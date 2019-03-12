package ch.epfl.dias.store.column;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import ch.epfl.dias.store.DataType;

public class DBColumn {

	private List<Object> col_data;

	public DBColumn(){
		this.col_data = new ArrayList<>();
	}

	public void add_elem(Object elem){
		this.col_data.add(elem);
	}


	public Integer[] getAsInteger() {
		Integer[] ret = new Integer[this.col_data.size()];

		for(int i = 0; i < this.col_data.size(); i++){
			ret[i] = Integer.parseInt((String)this.col_data.get(i));
		}
		return ret;
	}

	public Double[] getAsDouble() {
		Double[] ret = new Double[this.col_data.size()];

		for(int i = 0 ; i < this.col_data.size(); i++){
			ret[i] = Double.parseDouble((String)this.col_data.get(i));
		}
		return ret;
	}

	public Boolean[] getAsBoolean() {
		Boolean[] ret = new Boolean[this.col_data.size()];

		for(int i = 0; i < this.col_data.size(); i++){
			ret[i] = Boolean.parseBoolean((String)this.col_data.get(i));
		}
		return ret;
	}

	public String[] getAsString() {
		String[] ret = new String[this.col_data.size()];

		for(int i = 0; i < this.col_data.size(); i++){
			ret[i] = (String)this.col_data.get(i);
		}
		return ret;
	}

	@Override
	public String toString() {
		String ret = "[";

		for(int i=0; i < this.col_data.size(); i++){
			ret += String.valueOf(this.col_data.get(i));
			ret += ", ";
		}

		ret += "]\n";
		return ret;
	}
}

