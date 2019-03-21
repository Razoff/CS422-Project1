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
		this.col_data = new DBColumn[this.schema.length];

		// Init array
		for (int i=0; i < col_data.length; i++){
			col_data[i] = new DBColumn(this.schema[i]);
		}

		for (String line = reader.readLine(); line != null; line = reader.readLine()) {
			String[] tuple = line.split(this.delimiter);

			for (int i = 0; i < tuple.length; i++){
				this.col_data[i].add_elem(tuple[i]);
			}
		}
	}

	@Override
	public DBColumn[] getColumns(int[] columnsToGet) {
		DBColumn[] ret = new DBColumn[columnsToGet.length];
		for(int i=0; i < columnsToGet.length; i++){
			ret[i] = this.col_data[columnsToGet[i]];
		}

		return ret;
	}

	public DataType[] getSchema() {
		return schema;
	}

	public DBColumn[] getCol_data() {
		return col_data;
	}

	public boolean isLateMaterialization() {
		return lateMaterialization;
	}
}
