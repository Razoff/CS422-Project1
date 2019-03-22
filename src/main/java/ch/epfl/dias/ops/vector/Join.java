package ch.epfl.dias.ops.vector;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.stream.Stream;

public class Join implements VectorOperator {

	private VectorOperator leftChild;
	private VectorOperator rightChild;
	private int leftFieldNo;
	private int rightFieldNo;
	private DBColumn[] merge;
	private int next_size;
	private int last_returned;
	// since we dont know how many tuples we are supposed to pass through we will do an educated guess and pass
	// the biggest next we got

	public Join(VectorOperator leftChild, VectorOperator rightChild, int leftFieldNo, int rightFieldNo) {
		this.leftChild = leftChild;
		this.rightChild = rightChild;
		this.leftFieldNo = leftFieldNo;
		this.rightFieldNo = rightFieldNo;
	}

	@Override
	public void open() {
		this.leftChild.open();
		this.rightChild.open();
		this.merge = null;
		this.next_size = 0;
		this.last_returned=0;
	}

	@Override
	public DBColumn[] next() {
		if (this.merge == null){
			this.merge = this.execute();
		}

		if (!(this.last_returned < this.merge[0].getElemNumber())){
			return new DBColumn[]{new DBColumn()};
		}

		DBColumn[] ret = new DBColumn[this.merge.length];

		for (int i=0; i<ret.length;i++){
			ret[i] = new DBColumn(this.merge[i].getDataType(), false);
		}

		for(int i=0; i<this.next_size;i++){
			if (this.last_returned < this.merge[0].getElemNumber()) {
				for (int j = 0; j < this.merge.length; j++) {
					ret[j].add_elem(this.merge[j].getAsObject()[this.last_returned]);
				}
			}
			last_returned++;
		}
		return ret;
	}

	private DBColumn[] execute() {
		DBColumn[] leftCols = getAll(this.leftChild);
		DBColumn[] rightCols = getAll(this.rightChild);
		DBColumn left;
		DBColumn right;

		left = leftCols[this.leftFieldNo];
		right = rightCols[this.rightFieldNo];

		List<Integer> leftIDs = new ArrayList<>();
		List<Integer> rightIDs = new ArrayList<>();

		List<byte[]> leftHash = new ArrayList<>();
		List<byte[]> rightHash = new ArrayList<>();

		MessageDigest hashFunc;

		try {
			hashFunc = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
			return null;
		}


		List<Object> left_col_data = left.getCol_data();
		List<Object> right_col_data = right.getCol_data();

		for(int i=0; i<left.getElemNumber(); i++){
			leftHash.add(i, hashFunc.digest(left_col_data.get(i).toString().getBytes()));
		}
		for(int i=0; i<right.getElemNumber(); i++){
			rightHash.add(i, hashFunc.digest(right_col_data.get(i).toString().getBytes()));
		}

		for(int i=0; i< left.getElemNumber(); i++){
			for (int j=0; j< right.getElemNumber(); j++){
				if (Arrays.equals(leftHash.get(i), rightHash.get(j))){
					leftIDs.add(i);
					rightIDs.add(j);
				}
			}
		}
		return reconstruct(leftCols, rightCols, leftIDs, rightIDs);
	}

	private DBColumn[] reconstruct(DBColumn[] left, DBColumn[] right, List<Integer> leftIDs, List<Integer> rightIDs){
		DBColumn[] ret = new DBColumn[left.length + right.length];

		for (int i = 0; i < left.length; i++) {
			ret[i] = new DBColumn(left[i].getDataType(), false);
		}
		for (int i = 0; i < right.length; i++) {
			ret[i + left.length] = new DBColumn(right[i].getDataType(), false);
		}
		for (int i = 0; i < leftIDs.size(); i++) { // by construction leftIDs and rightIDs are the same size
			for (int j = 0; j < left.length; j++) {
				ret[j].add_elem(left[j].getCol_data().get(leftIDs.get(i)));
			}
			for (int j = 0; j < right.length; j++) {
				ret[j + left.length].add_elem(right[j].getCol_data().get(rightIDs.get(i)));
			}
		}
		return ret;
	}

	private DBColumn[] getAll(VectorOperator child){
		DBColumn[] elem = child.next();
		if (elem[0].eof){
			return new DBColumn[]{new DBColumn()};
		}
		if (elem[0].getElemNumber() > this.next_size){this.next_size = elem[0].getElemNumber();} // educated guess
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
			elem = child.next();
			if(elem[0].eof){
				return ret;
			}else{
				if (elem[0].getElemNumber() > this.next_size){this.next_size = elem[0].getElemNumber();} // educated guess
				for(int i=0; i<elem[0].getElemNumber(); i++){
					for (int j=0; j < ret.length; j++){
						ret[j].add_elem(elem[j].getAsObject()[i]);
					}
				}
			}
		}
	}

	@Override
	public void close() {
		this.leftChild.close();
		this.rightChild.close();
		this.last_returned = 0;
		this.next_size = 0;
	}
}
