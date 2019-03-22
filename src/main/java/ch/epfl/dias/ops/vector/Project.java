package ch.epfl.dias.ops.vector;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;

public class Project implements VectorOperator {

	private VectorOperator child;
	private int[] fieldNo;

	public Project(VectorOperator child, int[] fieldNo) {
		this.child = child;
		this.fieldNo = fieldNo;
	}

	@Override
	public void open() {
		this.child.open();
	}

	@Override
	public DBColumn[] next() {
		DBColumn[] elem = this.child.next();
		if(elem[0].eof) {
			return new DBColumn[]{new DBColumn()};
		}

		DBColumn[] ret = new DBColumn[this.fieldNo.length];

		for (int i = 0; i < this.fieldNo.length; i++) {
			ret[i] = elem[this.fieldNo[i]];
		}
		return ret;
	}

	@Override
	public void close() {
		this.child.close();
	}
}
