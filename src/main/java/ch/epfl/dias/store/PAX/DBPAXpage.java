package ch.epfl.dias.store.PAX;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;

import javax.xml.crypto.Data;
import java.lang.reflect.Array;
import java.util.Arrays;

public class DBPAXpage {

    private DataType[] schema;
    private int tuplesPerPage;
    private int currentTupleNumber;
    private int numberFields;
    private boolean isFull;
    private Object[] pax_data;

	DBPAXpage(DataType[] schema, int tuplesPerPage){
        this.schema = schema;
        this.tuplesPerPage = tuplesPerPage;

        this.currentTupleNumber = 0;
        this.numberFields = this.schema.length;
        this.isFull = false;

        this.pax_data = new Object[this.tuplesPerPage * this.numberFields];
    }

    public boolean add_elem(Object[] row){
	    if (this.isFull){
	        return false;
        }else{
            for(int i=0; i < this.numberFields ; i++){
	            pax_data[this.currentTupleNumber + (this.tuplesPerPage ) * i] = row[i];
            }
            this.currentTupleNumber++;

            if (this.currentTupleNumber == this.tuplesPerPage){
                this.isFull = true;
            }
	        return true;
        }
    }

    public Object[] getPax_data() {
        return pax_data;
    }

    @Override
    public String toString() {
        String ret = "Pax page metadata :\n";
        ret += "Tuples per pages : " + String.valueOf(this.tuplesPerPage) + "\n";
        ret += "Current number of tuples : " + String.valueOf(this.currentTupleNumber) + "\n";
        ret += "Content :\n";
        ret += Arrays.toString(this.pax_data);
        return ret;
    }
}
