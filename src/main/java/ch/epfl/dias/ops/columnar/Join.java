package ch.epfl.dias.ops.columnar;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.stream.Stream;

public class Join implements ColumnarOperator {

	private ColumnarOperator leftChild;
	private ColumnarOperator rightChild;
	private int leftFieldNo;
	private int rightFieldNo;

	public Join(ColumnarOperator leftChild, ColumnarOperator rightChild, int leftFieldNo, int rightFieldNo) {
		this.leftChild = leftChild;
		this.rightChild = rightChild;
		this.leftFieldNo = leftFieldNo;
		this.rightFieldNo = rightFieldNo;
	}

	public DBColumn[] execute() {
		DBColumn[] leftCols = this.leftChild.execute();
		DBColumn[] rightCols = this.rightChild.execute();
		DBColumn left;
		DBColumn right;
		if(leftCols[0].isLateMat() && rightCols[0].isLateMat()){ // both or none
			left = leftCols[this.leftFieldNo].lazyEval();
			right = rightCols[this.rightFieldNo].lazyEval();
		}else {
			left = leftCols[this.leftFieldNo];
			right = rightCols[this.rightFieldNo];
		}
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
		if (left[0].isLateMat() && right[0].isLateMat()){ // if only one is late mat then regular mat instead
			/*
			 * Here is how this unholy abomination works :
			 * left/right IDs holds the ids to be merge together -> {0,0,0} {1,2,3}
			 * We put those values in the "available IDs" of all left (repectively right) columns of the join
			 * with respect of what ID they currently refer to in the availIds of each column
			 */
			List<Integer> leftNewIDs = new ArrayList<>();
			List<Integer> rightNewIDs = new ArrayList<>();
			for (int i=0; i<leftIDs.size(); i++){
				leftNewIDs.add(left[this.leftFieldNo].availIDs.get(leftIDs.get(i)));
			}
			for (int i=0; i<rightIDs.size(); i++){
				rightNewIDs.add(right[this.rightFieldNo].availIDs.get(rightIDs.get(i)));
			}

			for (int i = 0; i < left.length; i++) {
				left[i].availIDs = leftNewIDs;
				ret[i] = left[i];
			}

			for (int i = 0; i < right.length; i++) {
				right[i].availIDs = rightNewIDs;
				ret[i + left.length] = right[i];
			}
			return ret;

		}else {
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
	}
}
