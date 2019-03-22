package ch.epfl.dias.ops.vector;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.ops.columnar.ColumnarOperator;
import ch.epfl.dias.ops.volcano.VolcanoOperator;
import ch.epfl.dias.store.column.DBColumn;

import java.util.ArrayList;
import java.util.List;

public class Select implements VectorOperator {

	private VectorOperator child;
	private BinaryOp op;
	private int fieldNo;
	private int value;
	private List<Integer> kept_ids;

	public Select(VectorOperator child, BinaryOp op, int fieldNo, int value) {
		this.child = child;
		this.op = op;
		this.fieldNo = fieldNo;
		this.value = value;
	}
	
	@Override
	public void open() {
		this.child.open();
		this.kept_ids = new ArrayList<>();
	}

	@Override
	public DBColumn[] next() {
		// A next call return only the response on the previous next call
		// It can hold 0 elems
		// But eof is call when everything is proceeded
		this.kept_ids = new ArrayList<>();
		DBColumn[] child_comp = this.child.next();
		if (child_comp[0].eof){
			return new DBColumn[]{new DBColumn()};
		}

		DBColumn elem = child_comp[this.fieldNo];
		Integer[] column_int;
		if(elem.isLateMat()){
			column_int = elem.lazyEval().getAsInteger();
		}else {
			column_int = elem.getAsInteger();
		}

		switch (this.op){
			case EQ:
				for(int i=0; i<column_int.length; i++){
					if(column_int[i] == this.value){
						this.kept_ids.add(i);
					}
				}
				break;
			case NE:
				for(int i=0; i<column_int.length; i++){
					if(column_int[i] != this.value){
						this.kept_ids.add(i);
					}
				}
				break;
			case LT:
				for(int i=0; i<column_int.length; i++){
					if(column_int[i] < this.value){
						this.kept_ids.add(i);
					}
				}
				break;
			case LE:
				for(int i=0; i<column_int.length; i++){
					if(column_int[i] <= this.value){
						this.kept_ids.add(i);
					}
				}
				break;
			case GT:
				for(int i=0; i<column_int.length; i++){
					if(column_int[i] > this.value){
						this.kept_ids.add(i);
					}
				}
				break;
			case GE:
				for(int i=0; i<column_int.length; i++){
					if(column_int[i] >= this.value){
						this.kept_ids.add(i);
					}
				}
				break;
		}
		return reconstruct(child_comp, column_int.length);

	}

	@Override
	public void close() {
		this.child.close();
	}

	private DBColumn[] reconstruct(DBColumn[] current_state, int nbIndices){

		DBColumn[] ret = new DBColumn[current_state.length];
		for (int i = 0; i < current_state.length; i++) {
			ret[i] = new DBColumn(current_state[i].getDataType(), false);
			Object[] elems = current_state[i].getAsObject();
			for (int o : this.kept_ids) {
				ret[i].add_elem(elems[o]);
			}
		}
		return ret;
	}
}
