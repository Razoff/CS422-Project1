package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.row.DBTuple;

public class Project implements VolcanoOperator {

	private VolcanoOperator child;
	private int[] fieldNo;

	public Project(VolcanoOperator child, int[] fieldNo) {
		this.child = child;
		this.fieldNo = fieldNo;
	}

	@Override
	public void open() {
		this.child.open();
	}

	@Override
	public DBTuple next() {
		DBTuple elem = this.child.next();

		if (elem.eof){
			return new DBTuple();
		}else{
			int i = 0;
			Object[] ret1 = new Object[this.fieldNo.length];
			DataType[] ret2 = new DataType[this.fieldNo.length];

			for (int x : this.fieldNo){
				ret1[i] = elem.fields[i];
				ret2[i] = elem.types[i];
				i++;
			}
			return new DBTuple(ret1,ret2);
		}
	}

	@Override
	public void close() {
		this.child.close();
	}
}
