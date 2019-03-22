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

	private DBColumn[] reconstruct(DBColumn[] current_state, int nbIndices){
		/*
		 * Now this is tricky so here is how this unholy abomination works
		 * we look at which indexes in the availIDs we keep
		 * but since other operations might have happenend before
		 * we want to match the position in the array not the value itself
		 * everytime we match we have to offset one bit
		 * EX two columsn with three element c1 = {0,1,2} (due to lazy eval) c2 {1,5,7} (from previous exec
		 * let's say that only 1 match -> 0 is not in [1] -> remove c1[0-0] c2[0-0] offset = 1
		 * 1 match do nothing
		 * 2 is not in [1] remove c1[2-1] c2 [2-1] -> final state c1={1} c2={5}
		 */
		if (current_state[this.fieldNo].isLateMat()){
			int offset = 0;
			for(int i = 0; i < nbIndices; i++){
				if (!this.kept_ids.contains(i)){
					for(int j = 0; j < current_state.length; j++){
						current_state[j].availIDs.remove(i + offset);
					}
					offset--;
				}
			}
			return current_state;
		}else {
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
}
