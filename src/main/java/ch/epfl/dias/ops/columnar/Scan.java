package ch.epfl.dias.ops.columnar;

import ch.epfl.dias.store.column.ColumnStore;
import ch.epfl.dias.store.column.DBColumn;

import java.util.stream.IntStream;

public class Scan implements ColumnarOperator {

	private ColumnStore store;

	public Scan(ColumnStore store) {
		this.store = store;
	}

	@Override
	public DBColumn[] execute() {
		if (this.store.isLateMaterialization()){
			return null;
		}else{
			//int[] a = IntStream.range(0, this.store.getCol_data().length).toArray();
			return this.store.getCol_data();
		}

	}
}
