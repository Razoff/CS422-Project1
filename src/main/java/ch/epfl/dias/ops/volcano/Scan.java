package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.row.DBTuple;

public class Scan implements VolcanoOperator {

	private Store store;
	private int current_row;

	public Scan(Store store) {
		this.store = store;
		this.current_row = 0;
	}

	@Override
	public void open() {
		// TODO: Implement
		// I am not sure it supposed to do anything
		this.current_row = 0;
	}

	@Override
	public DBTuple next() {
		try{
			DBTuple ret = this.store.getRow(this.current_row);
			this.current_row++;
			return ret;
		}catch (Exception e){
			return new DBTuple();
		}
	}

	@Override
	public void close() {
		// TODO: Implement
		// I am not sure it is supposed to do anything
	}
}