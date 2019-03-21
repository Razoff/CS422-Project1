package ch.epfl.dias.store.column;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;

import ch.epfl.dias.store.DataType;

public class DBColumn {

	private List<Object> col_data;
	private DataType dataType;
	public List<Integer> availIDs; // USE ONLY WITH LATE MAT
	private int last_id; // USE ONLY FOR LATE MAT
	private boolean lateMat;
	public boolean eof;

	public DBColumn(DataType dataType, boolean lateMat){
		this.dataType = dataType;
		this.col_data = new ArrayList<>();
		this.eof = false;
		this.lateMat = lateMat;
		this.availIDs = new ArrayList<>();
		this.last_id = 0;
	}

	public DBColumn(){this.eof = true;}

	public void add_elem(Object elem){
		this.col_data.add(elem);
		this.availIDs.add(this.last_id);
		this.last_id++;
	}

	public Integer[] getAsInteger() {
		Integer[] ret = new Integer[this.col_data.size()];

		for(int i = 0; i < this.col_data.size(); i++){
			ret[i] = Integer.parseInt(this.col_data.get(i).toString());
		}
		return ret;
	}

	public Double[] getAsDouble() {
		Double[] ret = new Double[this.col_data.size()];

		for(int i = 0 ; i < this.col_data.size(); i++){
			ret[i] = Double.parseDouble(this.col_data.get(i).toString());
		}
		return ret;
	}

	public Boolean[] getAsBoolean() {
		Boolean[] ret = new Boolean[this.col_data.size()];

		for(int i = 0; i < this.col_data.size(); i++){
			ret[i] = Boolean.parseBoolean(this.col_data.get(i).toString());
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

	public Object[] getAsObject() {
		Object[] ret = new Object[this.col_data.size()];

		for (int i=0; i < this.col_data.size(); i++){
			ret[i] = this.col_data.get(i);
		}
		return ret;
	}

	public DataType getDataType() {
		return dataType;
	}

	public int getElemNumber(){
		return this.col_data.size();
	}

	public List<Object> getCol_data() {
		return col_data;
	}

	public boolean isLateMat() {
		return lateMat;
	}

	public Double[] getStats(){
		Double[] ret = new Double[3]; // {Min, Max, Sum}
		switch (this.dataType) {
			case DOUBLE:
				Double[] d_ret = getAsDouble();
				Arrays.sort(d_ret);
				ret[0] = d_ret[0];
				ret[1] = d_ret[d_ret.length-1];
				ret[2] = 0.0;

				for (double e: d_ret){
					ret[2] += e;
				}
				break;
			case INT:
				Integer[] i_ret = getAsInteger();
				Arrays.sort(i_ret);
				ret[0] = Double.valueOf(i_ret[0]);
				ret[1] = Double.valueOf(i_ret[i_ret.length-1]);
				ret[2] = 0.0;

				for (int ei: i_ret){
					ret[2] += Double.valueOf(ei);
				}
				break;
			default:
				ret[0] = 0.0;
				ret[1] = 0.0;
				ret[2] = 0.0;
				break;
		}
		return ret;

	}

	public DBColumn lazyEval(){ // Use only on late mat
		DBColumn ret = new DBColumn(this.dataType, true);
		for(int indice : this.availIDs){
			ret.add_elem(this.col_data.get(indice));
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

