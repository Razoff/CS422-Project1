package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.row.DBTuple;

public class Select implements VolcanoOperator {

	private VolcanoOperator child;
	private BinaryOp op;
	private int fieldNo;
	private int value;

	public Select(VolcanoOperator child, BinaryOp op, int fieldNo, int value) {
		this.child = child;
		this.op = op;
		this.fieldNo = fieldNo;
		this.value = value;
	}

	@Override
	public void open() {
		this.child.open();
	}

	@Override
	public DBTuple next() {
		while(true){
			DBTuple elem = this.child.next();
			if (elem.eof){
				return elem;
			}else{
				switch (this.op) {
					case EQ:
						if(elem.getFieldAsInt(this.fieldNo) == this.value){
							return elem;
						}
						break;
					case GE:
						if(elem.getFieldAsInt(this.fieldNo) >= this.value){
							return elem;
						}
						break;
					case GT:
						if(elem.getFieldAsInt(this.fieldNo) > this.value){
							return elem;
						}
						break;
					case LE:
						if(elem.getFieldAsInt(this.fieldNo) <= this.value){
							return elem;
						}
						break;
					case LT:
						if(elem.getFieldAsInt(this.fieldNo) < this.value){
							return elem;
						}
						break;
					case NE:
						if(elem.getFieldAsInt(this.fieldNo) != this.value){
							return elem;
						}
						break;
				}
			}
		}
	}

	@Override
	public void close() {
		this.child.close();
	}
}
