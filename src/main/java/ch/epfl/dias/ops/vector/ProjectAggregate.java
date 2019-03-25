package ch.epfl.dias.ops.vector;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;

public class ProjectAggregate implements VectorOperator {

	private VectorOperator child;
	private Aggregate agg;
	private DataType dt;
	private int fieldNo;

	public ProjectAggregate(VectorOperator child, Aggregate agg, DataType dt, int fieldNo) {
		this.child = child;
		this.agg = agg;
		this.dt = dt;
		this.fieldNo = fieldNo;
	}

	@Override
	public void open() {
		this.child.open();
	}

	@Override
	public DBColumn[] next() {
		DBColumn[] curr_exec = this.getAll();
		DBColumn column = curr_exec[this.fieldNo];
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

			case MAX: // depends
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

	@Override
	public void close() {
		this.child.close();
	}

	private DBColumn[] getAll(){
		DBColumn[] elem;
		while(true){
			elem = this.child.next();
			if(elem != null){
				break;
			}
		}

		if (elem[0].eof){
			return new DBColumn[]{new DBColumn()};
		}
		DBColumn[] ret = new DBColumn[elem.length];
		for(int i=0; i<elem.length; i++){
			ret[i] = new DBColumn(elem[i].getDataType(), false); // init
		}
		for(int i=0; i<elem[0].getElemNumber(); i++){
			for (int j=0; j < ret.length; j++){
				ret[j].add_elem(elem[j].getAsObject()[i]);
			}
		}

		while(true){
			elem = this.child.next();
			if(elem[0].eof){
				return ret;
			}else{
				for(int i=0; i<elem[0].getElemNumber(); i++){
					for (int j=0; j < ret.length; j++){
						ret[j].add_elem(elem[j].getAsObject()[i]);
					}
				}
			}
		}
	}

}
