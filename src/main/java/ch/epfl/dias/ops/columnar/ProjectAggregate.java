package ch.epfl.dias.ops.columnar;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;

public class ProjectAggregate implements ColumnarOperator {

	private ColumnarOperator child;
	private Aggregate agg;
	private DataType dt;
	private int fieldNo;
	
	public ProjectAggregate(ColumnarOperator child, Aggregate agg, DataType dt, int fieldNo) {
		this.child = child;
		this.agg = agg;
		this.dt = dt;
		this.fieldNo = fieldNo;
	}

	@Override
	public DBColumn[] execute() {
		DBColumn[] curr_exec = this.child.execute();
		DBColumn column;
		if(curr_exec[0].isLateMat()){ // lazy eval to get the right stats
			column = curr_exec[this.fieldNo].lazyEval();
		}else{
			column = curr_exec[this.fieldNo];
		}
		DBColumn double_ret = new DBColumn(DataType.DOUBLE, false);
		DBColumn int_ret = new DBColumn(DataType.INT, false);
		Double[] stats = column.getStats(); // {min, max, sum}
		switch (this.agg){
			case SUM: // depens
				if (dt == DataType.INT){
					int_ret.add_elem(stats[2].intValue());
					return new DBColumn[]{int_ret};
				}else if(dt == DataType.DOUBLE){
					double_ret.add_elem(stats[2]);
					return new DBColumn[]{double_ret};
				}else{
					int_ret.add_elem(0);
					return new DBColumn[]{int_ret};
				}

			case MAX: // depens
				if (dt == DataType.INT){
					int_ret.add_elem(stats[1].intValue());
					return new DBColumn[]{int_ret};
				}else if (dt == DataType.DOUBLE) {
					double_ret.add_elem(stats[1]);
					return new DBColumn[]{double_ret};
				}else{
					int_ret.add_elem(0);
					return new DBColumn[]{int_ret};
				}
			case AVG: // double
				if (column.getElemNumber() != 0) { // prevent division by 0
					double_ret.add_elem(stats[2] / (double) column.getElemNumber());
					return new DBColumn[]{double_ret};
				}else{
					double_ret.add_elem(0.0);
					return new DBColumn[]{double_ret};
				}
			case COUNT: // int
				int_ret.add_elem(column.getElemNumber());
				return new DBColumn[]{int_ret};
			case MIN: // depends
				if (dt == DataType.INT){
					int_ret.add_elem(stats[0].intValue());
					return new DBColumn[]{int_ret};
				}else if (dt == DataType.DOUBLE) {
					double_ret.add_elem(stats[0]);
					return new DBColumn[]{double_ret};
				}else{
					int_ret.add_elem(0);
					return new DBColumn[]{int_ret};
				}
		}
		return null;
	}
}
