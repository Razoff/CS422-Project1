package ch.epfl.dias.ops.columnar;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.column.DBColumn;

public class Select implements ColumnarOperator {

	private ColumnarOperator child;
	private BinaryOp op;
	private int fieldNo;
	private int value;
	private List<Integer> kept_ids;

	public Select(ColumnarOperator child, BinaryOp op, int fieldNo, int value) {
		this.child = child;
		this.op = op;
		this.fieldNo = fieldNo;
		this.value = value;
		this.kept_ids = new ArrayList<>();
	}

	@Override
	public DBColumn[] execute() {
		DBColumn[] child_comp = this.child.execute();
		DBColumn elem = child_comp[this.fieldNo];
		Integer[] column_int = elem.getAsInteger();

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
		return reconstruct(child_comp);
	}

	private DBColumn[] reconstruct(DBColumn[] current_state){
		DBColumn[] ret = new DBColumn[current_state.length];
		for(int i=0; i<current_state.length;i++){
			ret[i] = new DBColumn(current_state[i].getDataType());
			Object[] elems = current_state[i].getAsObject();
			for (int o: this.kept_ids){
				ret[i].add_elem(elems[o]);
			}
		}
		return ret;
	}
}
