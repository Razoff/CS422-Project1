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
	private List<DBPAXpage> pax_data = new ArrayList<>();


	public PAXStore(DataType[] schema, String filename, String delimiter, int tuplesPerPage) {
		this.schema = schema;
		this.filename = filename;
		this.delimiter = delimiter;
		this.tuplesPerPage = tuplesPerPage;
	}

	@Override
	public void load() throws IOException {
		if(pax_data.isEmpty()){
			add_page();
		}

		BufferedReader reader = new BufferedReader(new FileReader(this.filename));
		Object[] data = new Object[schema.length];
		int page_no = 0;

		for (String line = reader.readLine(); line != null; line = reader.readLine()) {
			data = line.split(this.delimiter);
			if(!this.pax_data.get(page_no).add_elem(data)){
				add_page();
				page_no++;
				this.pax_data.get(page_no).add_elem(data);
			}
		}

		for(int i = 0 ; i < this.pax_data.size() ; i++){
			System.out.println(this.pax_data.get(i));
		}
	}

	private void add_page(){
		this.pax_data.add(new DBPAXpage(this.schema, this.tuplesPerPage));
	}

	@Override
	public DBTuple getRow(int rownumber) {
		int page = rownumber / this.tuplesPerPage; // Integer division
		int offset = rownumber % this.tuplesPerPage ; // Arrays start at 0

		/*if(offset == -1){
			page--;
			offset = this.tuplesPerPage- 1 ;
		}*/

		//System.out.println(this.pax_data.get(page));
		//System.out.println(offset);

		Object[] data = new Object[this.schema.length];

		for(int i=0; i<this.schema.length; i++){
			data[i] = this.pax_data.get(page).getPax_data()[offset + i * this.tuplesPerPage];
		}

		return new DBTuple(data, this.schema);
	}
}
