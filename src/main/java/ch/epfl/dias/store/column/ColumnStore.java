package ch.epfl.dias.store.column;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.Store;

public class ColumnStore extends Store {

	// TODO: Add required structures
	private DataType[] schema;
	private String filename;
	private String delimiter;
	private boolean lateMaterialization;
	private DBColumn[] col_data;

	public ColumnStore(DataType[] schema, String filename, String delimiter) {
		this(schema, filename, delimiter, false);
	}

	public ColumnStore(DataType[] schema, String filename, String delimiter, boolean lateMaterialization) {
		this.schema = schema;
		this.filename = filename;
		this.delimiter = delimiter;
		this.lateMaterialization = lateMaterialization;
		col_data = new DBColumn[schema.length]; // Number of elem in schema = number of columns
	}

	@Override
	public void load() throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(this.filename));

		for (String line = reader.readLine(); line != null; line = reader.readLine()) {
			String[] tuple = line.split(this.delimiter);

			for (int i = 0; i < tuple.length; i++){
				// TODO Implement columns first
			}
		}
	}

	@Override
	public DBColumn[] getColumns(int[] columnsToGet) {
		// TODO: Implement
		return null;
	}

	private Object elemParser(String elem, DataType type) throws IOException{
		Object ret = null;
		switch (type){
			case INT:
				ret = Integer.parseInt(elem);
				break;
			case DOUBLE:
				ret = Double.parseDouble(elem);
				break;
			case STRING:
				ret = elem; // Same type
				break;
			case BOOLEAN:
				ret = Boolean.parseBoolean(elem);
				break;
			default:
				throw new IOException("Error");
		}

		return ret;
	}
}
