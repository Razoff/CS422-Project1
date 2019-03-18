package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.row.DBTuple;

import java.util.*;
import java.util.stream.Stream;

import java.security.*;


public class HashJoin implements VolcanoOperator {

	private VolcanoOperator leftChild;
	private VolcanoOperator rightChild;
	private int leftFieldNo;
	private int rightFieldNo;

	private List<byte[]> leftHash;
	private List<byte[]> rightHash;

	private List<DBTuple> leftElems;
	private List<DBTuple> rightElems;

	private MessageDigest hashFunc;

	private int i;
	private int j;


	public HashJoin(VolcanoOperator leftChild, VolcanoOperator rightChild, int leftFieldNo, int rightFieldNo) {
		this.leftChild  = leftChild;
		this.rightChild = rightChild;
		this.leftFieldNo = leftFieldNo;
		this.rightFieldNo = rightFieldNo;

		this.leftHash = new ArrayList<>();
		this.rightHash = new ArrayList<>();

		this.leftElems = new ArrayList<>();
		this.rightElems = new ArrayList<>();

		this.i = 0;
		this.j = 0;

		try {
			this.hashFunc = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void open() {
		this.leftChild.open();
		this.rightChild.open();

		DBTuple leftElem;
		DBTuple righElem;

		while(true){
			leftElem = this.leftChild.next();
			righElem = this.rightChild.next();

			if(leftElem.eof && righElem.eof){
				break;
			}else {
				if (!leftElem.eof) {
					this.leftHash.add(0, hashFunc.digest(leftElem.getFieldAsCastString(this.leftFieldNo).getBytes()));
					this.leftElems.add(0, leftElem);
				}
				if (!righElem.eof){
					this.rightHash.add(0, hashFunc.digest(righElem.getFieldAsCastString(this.rightFieldNo).getBytes()));
					this.rightElems.add(0, righElem);
				}
			}
		}

	}

	@Override
	public DBTuple next() {
		while(this.i < this.leftHash.size()){
			while(this.j < this.rightHash.size()){
				if(Arrays.equals(this.leftHash.get(this.i), this.rightHash.get(this.j))){
					DBTuple ret = concatDBtuple(this.leftElems.get(this.i), this.rightElems.get(this.j));
					if (this.j == this.rightHash.size() -1 ){
						this.j = 0;
						this.i++;
					}else{
						this.j++;
					}
					return ret;
				}
				if (this.j == this.rightHash.size() -1 ){
					this.j = 0;
					this.i++;
				}else{
					this.j++;
				}
			}
		}
		return new DBTuple();
	}

	@Override
	public void close() {
		this.leftChild.close();
		this.rightChild.close();
	}

	// Function that concat two DBtuples for the hashjoin
	private DBTuple concatDBtuple(DBTuple e1, DBTuple e2){
		int len_e1 = e1.fields.length;
		int len_e2 = e2.fields.length;

		Object[] ret_fields = new Object[len_e1 + len_e2];
		DataType[] ret_schema = new DataType[len_e1 + len_e2];

		// Concat fields
		System.arraycopy(e1.fields, 0, ret_fields, 0, len_e1);
		System.arraycopy(e2.fields, 0, ret_fields, len_e1, len_e2);

		// Concat types
		System.arraycopy(e1.types, 0, ret_schema, 0, len_e1);
		System.arraycopy(e2.types, 0, ret_schema, len_e1, len_e2);

		return new DBTuple(ret_fields, ret_schema);

	}
}
