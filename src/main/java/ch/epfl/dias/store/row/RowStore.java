package ch.epfl.dias.store.row;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;

public class RowStore extends Store {

	// TODO: Add required structures
	private DataType[] schema;
	private String filename;
	private String delimiter;

	private List<DBTuple> row_data = new ArrayList<>();

	public RowStore(DataType[] schema, String filename, String delimiter) {
		this.schema = schema;
		this.filename = filename;
		this.delimiter = delimiter;
	}

	@Override
	public void load() throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(this.filename));

		for (String line = reader.readLine(); line != null; line = reader.readLine()) {
			row_data.add(new DBTuple(stringToObj(line.split(this.delimiter)) ,this.schema));
		}

	}

	@Override
	public DBTuple getRow(int rownumber) {
		return row_data.get(rownumber);
	}

	private Object[] stringToObj(String[] array) throws IOException{
		Object[] ret = new Object[array.length];

		for (int i = 0; i < array.length; i++){
			switch (schema[i]){
				case INT:
					ret[i] = Integer.parseInt(array[i]);
					break;
				case DOUBLE:
					ret[i] = Double.parseDouble(array[i]);
					break;
				case STRING:
					ret[i] = array[i]; // Field is already an int
					break;
				case BOOLEAN:
					ret[i] = Boolean.parseBoolean(array[i]);
					break;
  				default:
  					throw new IOException("Bad IO");

			}
		}

		return ret;
	}
}
