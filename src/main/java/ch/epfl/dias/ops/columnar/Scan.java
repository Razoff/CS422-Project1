package ch.epfl.dias.ops.columnar;

import ch.epfl.dias.store.column.ColumnStore;
import ch.epfl.dias.store.column.DBColumn;

import java.util.Arrays;
import java.util.stream.IntStream;

public class Scan implements ColumnarOperator {

	private ColumnStore store;
	public Scan(ColumnStore store) {
		this.store = store;
	}

	@Override
	public DBColumn[] execute() {
    	return this.store.getCol_data();
	}
}
