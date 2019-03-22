package ch.epfl.dias.ops.vector;

import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.column.DBColumn;

public class Scan implements VectorOperator {

	private Store store;
	private int vectorsize;
	private DBColumn[] columns;

	private int last_ret_id;

	public Scan(Store store, int vectorsize) { // Only early mat
		this.store = store;
		this.vectorsize = vectorsize;
		this.last_ret_id = 0;
		this.columns = this.store.getColumns(null);
    }
	
	@Override
	public void open() {
		this.last_ret_id = 0;
        this.columns = this.store.getColumns(null);
    }

	@Override
	public DBColumn[] next() {
	    // If everything is already return return eof
	    if (!(this.last_ret_id < columns[0].getElemNumber())){
	        return new DBColumn[]{new DBColumn()};
        }
	    // Else return next vectorsize elem
		DBColumn[] ret = new DBColumn[this.columns.length];
		for (int i=0; i<this.columns.length; i++){
		    ret[i] = new DBColumn(this.columns[i].getDataType(), false);
        }
		for (int i=0; i<this.vectorsize; i++){
		    if (last_ret_id < columns[0].getElemNumber()){
		        for (int j=0; j<this.columns.length; j++){
                    ret[j].add_elem(this.columns[j].getAsObject()[this.last_ret_id]);
                }
            }
		    this.last_ret_id++;
        }
		// The return might be smaller than vectorsize but it is not empty
	    return ret;
	}

	@Override
	public void close() {
		this.last_ret_id = 0;
	}
}
