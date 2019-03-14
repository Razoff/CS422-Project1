package ch.epfl.dias.store.PAX;

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
import ch.epfl.dias.store.row.DBTuple;

public class PAXStore extends Store {

	private DataType[] schema;
	private String filename;
	private String delimiter;
	private int tuplesPerPage;
	private int elemPerTuple;
	private List<DBPAXpage> pax_data = new ArrayList<>();


	public PAXStore(DataType[] schema, String filename, String delimiter, int tuplesPerPage) {
		this.schema = schema;
		this.filename = filename;
		this.delimiter = delimiter;
		this.tuplesPerPage = tuplesPerPage;
		this.elemPerTuple = schema.length;
	}

	@Override
	public void load() throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(this.filename));
		DBPAXpage paax = new DBPAXpage(schema, 11);
		Object[] data = new Object[schema.length];
		for (String line = reader.readLine(); line != null; line = reader.readLine()) {
			data = line.split(this.delimiter);
			paax.add_elem(data);
		}

		System.out.println(paax);
	}

	@Override
	public DBTuple getRow(int rownumber) {
		// TODO: Implement
		return null;
	}
}
