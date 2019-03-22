package ch.epfl.dias;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.PAX.PAXStore;
import ch.epfl.dias.store.column.ColumnStore;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;
import ch.epfl.dias.store.row.RowStore;

import java.io.IOException;
import java.util.Arrays;

public class Main {

	public static void main(String[] args) {

		/*DataType[] schema = new DataType[] { DataType.INT, DataType.INT, DataType.INT, DataType.INT, DataType.INT,
				DataType.INT, DataType.INT, DataType.INT, DataType.INT, DataType.INT };

		DataType[] orderSchema = new DataType[] { DataType.INT, DataType.INT, DataType.STRING, DataType.DOUBLE,
				DataType.STRING, DataType.STRING, DataType.STRING, DataType.INT, DataType.STRING };

		schema = new DataType[] { DataType.INT, DataType.INT, DataType.INT, DataType.INT, DataType.INT, DataType.INT,
				DataType.INT, DataType.INT, DataType.INT, DataType.INT };

		RowStore rowstore = new RowStore(orderSchema, "input/orders_small.csv", "\\|");
		try {
			rowstore.load();
		} catch (IOException e){
			System.out.println(e);
		}

		// MY CODE

		//System.out.println(rowstore.getRow(0));

		ColumnStore colstore = new ColumnStore(orderSchema, "input/orders_small.csv", "\\|");
		try{
			colstore.load();
		}catch (IOException e){
			System.out.println(e);
		}

		int[] sa = new int[]{4};

		//System.out.println(colstore.getColumns(sa)[0]);

		// END OF MY CODE

        PAXStore paxstore = new PAXStore(orderSchema, "input/orders_small.csv", "\\|", 3);
        try{
            paxstore.load();
        }catch (IOException e){
            System.out.println(e);
        }

        // MY CODE

		//System.out.println(paxstore.getRow(3));

		// END OF MY CODE

		ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(rowstore);
		DBTuple currentTuple = scan.next();
		while (!currentTuple.eof) {
		 	System.out.println(currentTuple.getFieldAsInt(1));
			currentTuple = scan.next();
		}

		ColumnStore columnstoreData = new ColumnStore(schema, "input/data.csv", ",");
		try {
			columnstoreData.load();
		}catch (IOException e){
			System.out.println(e);
		}

		ch.epfl.dias.ops.columnar.Scan c_scan = new ch.epfl.dias.ops.columnar.Scan(columnstoreData);
		//ch.epfl.dias.ops.columnar.Select sel = new ch.epfl.dias.ops.columnar.Select(c_scan, BinaryOp.EQ, 3, 6);
		//ch.epfl.dias.ops.columnar.ProjectAggregate agg = new ch.epfl.dias.ops.columnar.ProjectAggregate(sel, Aggregate.COUNT, DataType.INT, 2);
		// DBColumn[] result = agg.execute();
		// int output = result[0].getAsInteger()[0];
		// System.out.println(output);

		DBColumn[] result = c_scan.execute();
		System.out.println(Arrays.toString(result));*/

		DataType[] orderSchema;
		DataType[] lineitemSchema;
		DataType[] schema;

		RowStore rowstoreOrder;
		RowStore rowstoreLineItem;

		PAXStore paxstoreOrder;
		PAXStore paxstoreLineItem;
		int nb_order_tuple = 200;
		int nb_line_tuple = 200;

		ColumnStore columnstoreOrder;
		ColumnStore columnstoreLineItem;

		orderSchema = new DataType[] { DataType.INT, DataType.INT, DataType.STRING, DataType.DOUBLE, DataType.STRING,
				DataType.STRING, DataType.STRING, DataType.INT, DataType.STRING };

		lineitemSchema = new DataType[] { DataType.INT, DataType.INT, DataType.INT, DataType.INT, DataType.DOUBLE,
				DataType.DOUBLE, DataType.DOUBLE, DataType.DOUBLE, DataType.STRING, DataType.STRING, DataType.STRING,
				DataType.STRING, DataType.STRING, DataType.STRING, DataType.STRING, DataType.STRING };

		try {
			rowstoreOrder = new RowStore(orderSchema, "input/orders_big.csv", "\\|");
			rowstoreOrder.load();

			rowstoreLineItem = new RowStore(lineitemSchema, "input/lineitem_big.csv", "\\|");
			rowstoreLineItem.load();

		}catch (Exception e){
			System.out.println(e);
		}finally {
			rowstoreOrder = null; // garbage collection
			rowstoreLineItem = null;
		}

		try {
			paxstoreOrder = new PAXStore(orderSchema, "input/orders_big.csv", "\\|", nb_order_tuple);
			paxstoreOrder.load();

			paxstoreLineItem = new PAXStore(lineitemSchema, "input/lineitem_big.csv", "\\|", nb_line_tuple);
			paxstoreLineItem.load();

		}catch (Exception e){
			System.out.println(e);
		}finally {
			paxstoreOrder = null; // garbage collection
			paxstoreLineItem = null;
		}

		try {
			columnstoreOrder = new ColumnStore(orderSchema, "input/orders_big.csv", "\\|");
			columnstoreOrder.load();

			columnstoreLineItem = new ColumnStore(lineitemSchema, "input/lineitem_big.csv", "\\|");
			columnstoreLineItem.load();

		}catch (Exception e){
			System.out.println(e);
		}finally {
			columnstoreOrder = null; // garbage collection
			columnstoreLineItem = null;
		}
	}
}
