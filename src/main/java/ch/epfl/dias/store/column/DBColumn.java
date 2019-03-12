package ch.epfl.dias.store.column;

import java.util.ArrayList;
import java.util.Arrays;

import ch.epfl.dias.store.DataType;

public class DBColumn {
	/*
	This will besaically be a list of object of any type
	Maybe it will also hold the type of the column who knows
	Probably will use the same logic as row stor and have four
	different getAs'bla' methods
	 */

	public Object[] fields;
	public DataType[] types;
	public boolean eof = false;

	public DBColumn(Object[] fields, Da type){
		this.fields = fields;
		this.types = types;
	}

	public DBColumn(){
		this.eof = true;

	public Integer[] getAsInteger() {
		Integer[] ret = new Integer[this.fields.length];

		for(int i = 0; i < this.fields.length; i++){
			ret[i] = (Integer)this.fields[i];
		}
		return ret;
	}

	public Double[] getAsDouble() {
		Double[] ret = new Double[this.fields.length];

		for(int i = 0 ; i < this.fields.length; i++){
			ret[i] = (Double)this.fields[i];
		}
		return ret;
	}

	public Boolean[] getAsBoolean() {
		Boolean[] ret = new Boolean[this.fields.length];

		for(int i = 0; i < this.fields.length; i++){
			ret[i] = (Boolean) this.fields[i];
		}
		return ret;
	}

	public String[] getAsString() {
		String[] ret = new String[this.fields.length];

		for(int i = 0; i < this.fields.length; i++){
			ret[i] = (String)this.fields[i];
		}
		return ret;
	}
}
}
