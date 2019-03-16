package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;
import ch.epfl.dias.store.row.DBTuple;

public class ProjectAggregate implements VolcanoOperator {

	private VolcanoOperator child;
	private Aggregate agg;
	private DataType dt;
	private int fieldNo;

	public ProjectAggregate(VolcanoOperator child, Aggregate agg, DataType dt, int fieldNo) {
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
	public DBTuple next() {
		double ret = 0; // Double by default parsed if needed before return
		DBTuple elem;
		switch (this.agg){
			case COUNT:
				while(!this.child.next().eof){
					ret++;
				}
				return new DBTuple(new Object[]{(int)ret}, new DataType[]{DataType.INT});
			case AVG:
				int count = 0;
				while (true){
					elem = this.child.next();

					if (elem.eof){

						if(count == 0){ // Avoid division by 0
							return new DBTuple(new Object[]{0.0}, new DataType[]{DataType.DOUBLE});
						}else{
							return new DBTuple(new Object[]{ret/(double)count}, new DataType[]{DataType.DOUBLE});
						}

					}else

						switch (this.dt){
							case INT:
								ret += elem.getFieldAsInt(this.fieldNo);
								count++;
								break;
							case DOUBLE:
								ret += elem.getFieldAsDouble(this.fieldNo);
								count++;
								break;
						}
				}
			case MAX:
				elem = this.child.next();
				if (elem.eof){
					return elem;
				}else{ // First elem to get base
					switch (this.dt){
						case DOUBLE:
							ret = elem.getFieldAsDouble(this.fieldNo);
							break;
						case INT:
							ret = elem.getFieldAsInt(this.fieldNo);
							break;
					}
				}

				while (true){
					elem = this.child.next();
					if (elem.eof) {
						switch (this.dt) {
							case DOUBLE:
								return new DBTuple(new Object[]{ret}, new DataType[]{DataType.DOUBLE});

							case INT:
								return new DBTuple(new Object[]{(int) ret}, new DataType[]{DataType.INT});
						}
					}else{
						switch (this.dt){
							case DOUBLE:
								if(elem.getFieldAsDouble(this.fieldNo) > ret){
									ret = elem.getFieldAsDouble(this.fieldNo);
								}
								break;
							case INT:
								if(elem.getFieldAsInt(this.fieldNo) > ret){
									ret = elem.getFieldAsInt(this.fieldNo);
								}
								break;
						}
					}
				}
			case MIN:
				elem = this.child.next();
				if (elem.eof){
					return elem;
				}else{ // First elem to get base
					switch (this.dt){
						case DOUBLE:
							ret = elem.getFieldAsDouble(this.fieldNo);
							break;
						case INT:
							ret = elem.getFieldAsInt(this.fieldNo);
							break;
					}
				}

				while (true){
					elem = this.child.next();
					if (elem.eof) {
						switch (this.dt) {
							case DOUBLE:
								return new DBTuple(new Object[]{ret}, new DataType[]{DataType.DOUBLE});

							case INT:
								return new DBTuple(new Object[]{(int) ret}, new DataType[]{DataType.INT});
						}
					}else{
						switch (this.dt){
							case DOUBLE:
								if(elem.getFieldAsDouble(this.fieldNo) < ret){
									ret = elem.getFieldAsDouble(this.fieldNo);
								}
								break;
							case INT:
								if(elem.getFieldAsInt(this.fieldNo) < ret){
									ret = elem.getFieldAsInt(this.fieldNo);
								}
								break;
						}
					}
				}
			case SUM:
				while (true){
					elem = this.child.next();

					if (elem.eof){
						switch (this.dt){
							case INT:
								return new DBTuple(new Object[]{(int)ret}, new DataType[]{DataType.INT});
							case DOUBLE:
								return new DBTuple(new Object[]{ret}, new DataType[]{DataType.DOUBLE});
						}

					}else

						switch (this.dt){
							case INT:
								ret += elem.getFieldAsInt(this.fieldNo);
								break;
							case DOUBLE:
								ret += elem.getFieldAsDouble(this.fieldNo);
								break;
						}
				}
		}
		return null;
	}

	@Override
	public void close() {
		this.child.close();
	}

}
