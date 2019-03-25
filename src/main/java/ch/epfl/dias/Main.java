package ch.epfl.dias;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.ops.volcano.HashJoin;
import ch.epfl.dias.ops.volcano.ProjectAggregate;
import ch.epfl.dias.ops.volcano.Select;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.PAX.PAXStore;
import ch.epfl.dias.store.column.ColumnStore;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;
import ch.epfl.dias.store.row.RowStore;
import java.util.concurrent.TimeUnit;

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
		int nb_order_tuple = 2000;
		int nb_line_tuple = 2000;

		ColumnStore columnstoreOrder;
		ColumnStore columnstoreLineItem;

		orderSchema = new DataType[] { DataType.INT, DataType.INT, DataType.STRING, DataType.DOUBLE, DataType.STRING,
				DataType.STRING, DataType.STRING, DataType.INT, DataType.STRING };

		lineitemSchema = new DataType[] { DataType.INT, DataType.INT, DataType.INT, DataType.INT, DataType.DOUBLE,
				DataType.DOUBLE, DataType.DOUBLE, DataType.DOUBLE, DataType.STRING, DataType.STRING, DataType.STRING,
				DataType.STRING, DataType.STRING, DataType.STRING, DataType.STRING, DataType.STRING };

		try {
			System.out.println("Row store order load");
			rowstoreOrder = new RowStore(orderSchema, "input/orders_big.csv", "\\|");
			rowstoreOrder.load();
			System.out.println("END");

			System.out.println("Row store line_item load");
			rowstoreLineItem = new RowStore(lineitemSchema, "input/lineitem_big.csv", "\\|");
			rowstoreLineItem.load();
			System.out.println("END");

			System.out.println("Query 1 :");
			System.out.println(queryOneRow(rowstoreLineItem));
			System.out.println("Query 2 :");
			System.out.println(queryTwoRow(rowstoreLineItem));
			System.out.println("Query 3 :");
			System.out.println(queryThreeRow(rowstoreLineItem));
			System.out.println("Query 4 :");
			System.out.println(queryFourRow(rowstoreLineItem));
			System.out.println("Query 5 :");
			System.out.println(queryFiveRow(rowstoreLineItem, rowstoreOrder));

		}catch (Exception e){
			System.out.println(e);
		}finally {
			System.out.println("Nullify row stores");
			rowstoreOrder = null; // garbage collection
			rowstoreLineItem = null;
		}

		try {
			System.out.println("Pax store order load");
			paxstoreOrder = new PAXStore(orderSchema, "input/orders_big.csv", "\\|", nb_order_tuple);
			/*paxstoreOrder.load();*/
			System.out.println("END");

			System.out.println("Pax store line_item load");
			paxstoreLineItem = new PAXStore(lineitemSchema, "input/lineitem_big.csv", "\\|", nb_line_tuple);
			paxstoreLineItem.load();
			System.out.println("END");

			System.out.println("Query 1 :");
			System.out.println(queryOnePax(paxstoreLineItem));
			System.out.println("Query 2 :");
			System.out.println(queryTwoPax(paxstoreLineItem));
			System.out.println("Query 3 :");
			System.out.println(queryThreePax(paxstoreLineItem));
			System.out.println("Query 4 :");
			System.out.println(queryFourPax(paxstoreLineItem));
			System.out.println("Query 5 :");
			System.out.println(queryFivePax(paxstoreLineItem, paxstoreOrder));



		}catch (Exception e){
			System.out.println(e);
		}finally {
			System.out.println("Nullify pax stores");
			paxstoreOrder = null; // garbage collection
			paxstoreLineItem = null;
		}

		try {
			System.out.println("Col store order load");
			columnstoreOrder = new ColumnStore(orderSchema, "input/orders_big.csv", "\\|");
			columnstoreOrder.load();
			System.out.println("END");

			System.out.println("Col store line_item load");
			columnstoreLineItem = new ColumnStore(lineitemSchema, "input/lineitem_big.csv", "\\|");
			columnstoreLineItem.load();
			System.out.println("END");

			System.out.println("Vector query");
			System.out.println("Query 1 :");
			System.out.println(queryOneVec(columnstoreLineItem));
			System.out.println("Query 2 :");
			System.out.println(queryTwoVec(columnstoreLineItem));
			System.out.println("Query 3 :");
			System.out.println(queryThreeVec(columnstoreLineItem));
			System.out.println("Query 4 :");
			System.out.println(queryFourVec(columnstoreLineItem));
			System.out.println("Query 5 :");
			System.out.println(queryFiveVec(columnstoreLineItem, columnstoreOrder));

			System.out.println("Columnar query");
			System.out.println("Query 1 :");
			System.out.println(queryOneCol(columnstoreLineItem));
			System.out.println("Query 2 :");
			System.out.println(queryTwoCol(columnstoreLineItem));
			System.out.println("Query 3 :");
			System.out.println(queryThreeCol(columnstoreLineItem));
			System.out.println("Query 4 :");
			System.out.println(queryFourCol(columnstoreLineItem));
			System.out.println("Query 5 :");
			System.out.println(queryFiveCol(columnstoreLineItem, columnstoreOrder));


		}catch (Exception e){
			System.out.println(e);
		}finally {
			System.out.println("Nullify col stores");
			columnstoreOrder = null; // garbage collection
			columnstoreLineItem = null;
		}
		System.out.println("Finished");
	}

	// ROW QUERY
	static public long queryOneRow(RowStore lineItem){
		// Select SELECT L_ORDERKEY, L_quantity FROM LINE_ITEM WHERE col4 == 6
		ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(lineItem);
		ch.epfl.dias.ops.volcano.Select sel = new ch.epfl.dias.ops.volcano.Select(scan, BinaryOp.EQ, 3, 6);
		ch.epfl.dias.ops.volcano.Project prj = new ch.epfl.dias.ops.volcano.Project(sel, new int[]{0,4});

		long startTime = System.nanoTime();
		prj.open();
		DBTuple result = prj.next();
		System.out.println(result);
		long endTime = System.nanoTime();
		return (endTime - startTime);
	}

	static public long queryTwoRow(RowStore lineItem){
		// Select SELECT L_ORDERKEY, L_quantity FROM LINE_ITEM WHERE col4 > 6
		ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(lineItem);
		ch.epfl.dias.ops.volcano.Select sel = new ch.epfl.dias.ops.volcano.Select(scan, BinaryOp.GT, 3, 6);
		ch.epfl.dias.ops.volcano.Project prj = new ch.epfl.dias.ops.volcano.Project(sel, new int[]{0,4});

		long startTime = System.nanoTime();
		prj.open();
		DBTuple result = prj.next();
		System.out.println(result);
		long endTime = System.nanoTime();
		return (endTime - startTime);
	}

	static public long queryThreeRow(RowStore lineItem){
		// SELECT COUNT (*) FROM LINE_ITEM
		ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(lineItem);
		ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(scan, Aggregate.COUNT, DataType.INT, 3);

		long startTime = System.nanoTime();
		agg.open();
		DBTuple result = agg.next();
		System.out.println(result);
		long endTime = System.nanoTime();
		return (endTime - startTime);
	}

	static public long queryFourRow(RowStore lineItem){
		// Select SELECT L_ORDERKEY, L_quantity FROM LINE_ITEM WHERE col4 > 6
		ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(lineItem);
		ch.epfl.dias.ops.volcano.Select sel = new ch.epfl.dias.ops.volcano.Select(scan, BinaryOp.GE, 3, 6);
		ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(sel, Aggregate.AVG, DataType.DOUBLE, 4);

		long startTime = System.nanoTime();
		agg.open();
		DBTuple result = agg.next();
		System.out.println(result);
		long endTime = System.nanoTime();
		return (endTime - startTime);
	}

	static public long queryFiveRow(RowStore lineItem, RowStore orders){
		// SELECT COUNT(*) FROM order JOIN lineitem ON (o_orderkey = orderkey)

		ch.epfl.dias.ops.volcano.Scan scanOrder = new ch.epfl.dias.ops.volcano.Scan(orders);
		ch.epfl.dias.ops.volcano.Scan scanLineitem = new ch.epfl.dias.ops.volcano.Scan(lineItem);

		HashJoin join = new HashJoin(scanOrder,scanLineitem,0,0);
		ProjectAggregate agg = new ProjectAggregate(join,Aggregate.COUNT, DataType.INT,0);

		long startTime = System.nanoTime();
		agg.open();
		DBTuple result = agg.next();
		System.out.println(result);
		long endTime = System.nanoTime();
		return (endTime - startTime);
	}

	// PAX QUERY
	static public long queryOnePax(PAXStore lineItem){
		// Select SELECT L_ORDERKEY, L_quantity FROM LINE_ITEM WHERE col4 == 6
		ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(lineItem);
		ch.epfl.dias.ops.volcano.Select sel = new ch.epfl.dias.ops.volcano.Select(scan, BinaryOp.EQ, 3, 6);
		ch.epfl.dias.ops.volcano.Project prj = new ch.epfl.dias.ops.volcano.Project(sel, new int[]{0,4});

		long startTime = System.nanoTime();
		prj.open();
		DBTuple result = prj.next();
		System.out.println(result);
		long endTime = System.nanoTime();
		return (endTime - startTime);
	}

	static public long queryTwoPax(PAXStore lineItem){
		// Select SELECT L_ORDERKEY, L_quantity FROM LINE_ITEM WHERE col4 > 6
		ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(lineItem);
		ch.epfl.dias.ops.volcano.Select sel = new ch.epfl.dias.ops.volcano.Select(scan, BinaryOp.GT, 3, 6);
		ch.epfl.dias.ops.volcano.Project prj = new ch.epfl.dias.ops.volcano.Project(sel, new int[]{0,4});

		long startTime = System.nanoTime();
		prj.open();
		DBTuple result = prj.next();
		System.out.println(result);
		long endTime = System.nanoTime();
		return (endTime - startTime);
	}

	static public long queryThreePax(PAXStore lineItem){
		// SELECT COUNT (*) FROM LINE_ITEM
		ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(lineItem);
		ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(scan, Aggregate.COUNT, DataType.INT, 3);

		long startTime = System.nanoTime();
		agg.open();
		DBTuple result = agg.next();
		System.out.println(result);
		long endTime = System.nanoTime();
		return (endTime - startTime);
	}

	static public long queryFourPax(PAXStore lineItem){
		// Select SELECT L_ORDERKEY, L_quantity FROM LINE_ITEM WHERE col4 > 6
		ch.epfl.dias.ops.volcano.Scan scan = new ch.epfl.dias.ops.volcano.Scan(lineItem);
		ch.epfl.dias.ops.volcano.Select sel = new ch.epfl.dias.ops.volcano.Select(scan, BinaryOp.GE, 3, 6);
		ch.epfl.dias.ops.volcano.ProjectAggregate agg = new ch.epfl.dias.ops.volcano.ProjectAggregate(sel, Aggregate.AVG, DataType.DOUBLE, 4);

		long startTime = System.nanoTime();
		agg.open();
		DBTuple result = agg.next();
		System.out.println(result);
		long endTime = System.nanoTime();
		return (endTime - startTime);
	}

	static public long queryFivePax(PAXStore lineItem, PAXStore orders){
		// SELECT COUNT(*) FROM order JOIN lineitem ON (o_orderkey = orderkey)

		ch.epfl.dias.ops.volcano.Scan scanOrder = new ch.epfl.dias.ops.volcano.Scan(orders);
		ch.epfl.dias.ops.volcano.Scan scanLineitem = new ch.epfl.dias.ops.volcano.Scan(lineItem);

		HashJoin join = new HashJoin(scanOrder,scanLineitem,0,0);
		ProjectAggregate agg = new ProjectAggregate(join,Aggregate.COUNT, DataType.INT,0);

		long startTime = System.nanoTime();
		agg.open();
		DBTuple result = agg.next();
		System.out.println(result);
		long endTime = System.nanoTime();
		return (endTime - startTime);
	}

	// COLUMN QUERY
	static public long queryOneCol(ColumnStore lineItem){
		// Select SELECT L_ORDERKEY, L_quantity FROM LINE_ITEM WHERE col4 == 6
		ch.epfl.dias.ops.columnar.Scan scan = new ch.epfl.dias.ops.columnar.Scan(lineItem);
		ch.epfl.dias.ops.columnar.Select sel = new ch.epfl.dias.ops.columnar.Select(scan, BinaryOp.EQ, 3, 6);
		ch.epfl.dias.ops.columnar.Project prj = new ch.epfl.dias.ops.columnar.Project(sel, new int[]{0,4});

		long startTime = System.nanoTime();
		DBColumn[] result = prj.execute();
		System.out.println(result[0]);
		long endTime = System.nanoTime();
		return (endTime - startTime);
	}

	static public long queryTwoCol(ColumnStore lineItem){
		// Select SELECT L_ORDERKEY, L_quantity FROM LINE_ITEM WHERE col4 > 6
		ch.epfl.dias.ops.columnar.Scan scan = new ch.epfl.dias.ops.columnar.Scan(lineItem);
		ch.epfl.dias.ops.columnar.Select sel = new ch.epfl.dias.ops.columnar.Select(scan, BinaryOp.GT, 3, 6);
		ch.epfl.dias.ops.columnar.Project prj = new ch.epfl.dias.ops.columnar.Project(sel, new int[]{0,4});

		long startTime = System.nanoTime();
		DBColumn[] result = prj.execute();
		System.out.println(result[0]);
		long endTime = System.nanoTime();
		return (endTime - startTime);
	}

	static public long queryThreeCol(ColumnStore lineItem){
		// SELECT COUNT (*) FROM LINE_ITEM
		ch.epfl.dias.ops.columnar.Scan scan = new ch.epfl.dias.ops.columnar.Scan(lineItem);
		ch.epfl.dias.ops.columnar.ProjectAggregate agg = new ch.epfl.dias.ops.columnar.ProjectAggregate(scan, Aggregate.COUNT, DataType.INT, 3);

		long startTime = System.nanoTime();
		DBColumn[] result = agg.execute();
		System.out.println(result[0]);
		long endTime = System.nanoTime();
		return (endTime - startTime);
	}

	static public long queryFourCol(ColumnStore lineItem){
		// Select SELECT L_ORDERKEY, L_quantity FROM LINE_ITEM WHERE col4 > 6
		ch.epfl.dias.ops.columnar.Scan scan = new ch.epfl.dias.ops.columnar.Scan(lineItem);
		ch.epfl.dias.ops.columnar.Select sel = new ch.epfl.dias.ops.columnar.Select(scan, BinaryOp.GE, 3, 6);
		ch.epfl.dias.ops.columnar.ProjectAggregate agg = new ch.epfl.dias.ops.columnar.ProjectAggregate(sel, Aggregate.AVG, DataType.DOUBLE, 4);

		long startTime = System.nanoTime();
		DBColumn[] result = agg.execute();
		System.out.println(result[0]);
		long endTime = System.nanoTime();
		return (endTime - startTime);
	}

	static public long queryFiveCol(ColumnStore lineItem, ColumnStore orders){
		// SELECT COUNT(*) FROM order JOIN lineitem ON (o_orderkey = orderkey)

		ch.epfl.dias.ops.columnar.Scan scanOrder = new ch.epfl.dias.ops.columnar.Scan(orders);
		ch.epfl.dias.ops.columnar.Scan scanLineitem = new ch.epfl.dias.ops.columnar.Scan(lineItem);

		ch.epfl.dias.ops.columnar.Join join = new ch.epfl.dias.ops.columnar.Join(scanLineitem, scanOrder, 0, 0);
		ch.epfl.dias.ops.columnar.ProjectAggregate agg = new ch.epfl.dias.ops.columnar.ProjectAggregate(join, Aggregate.COUNT, DataType.INT, 0);

		long startTime = System.nanoTime();
		DBColumn[] result = agg.execute();
		System.out.println(result[0]);
		long endTime = System.nanoTime();
		return (endTime - startTime);
	}

	// VECTOR QUERY
	static public long queryOneVec(ColumnStore lineItem){
		// Select SELECT L_ORDERKEY, L_quantity FROM LINE_ITEM WHERE col4 == 6
		ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(lineItem,5000);
		ch.epfl.dias.ops.vector.Select sel = new ch.epfl.dias.ops.vector.Select(scan, BinaryOp.EQ, 3, 6);
		ch.epfl.dias.ops.vector.Project prj = new ch.epfl.dias.ops.vector.Project(sel, new int[]{0,4});

		long startTime = System.nanoTime();
		prj.open();
		DBColumn[] result = prj.next();
		System.out.println(result[0]);
		long endTime = System.nanoTime();
		return (endTime - startTime);
	}

	static public long queryTwoVec(ColumnStore lineItem){
		// Select SELECT L_ORDERKEY, L_quantity FROM LINE_ITEM WHERE col4 > 6
		ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(lineItem,5000);
		ch.epfl.dias.ops.vector.Select sel = new ch.epfl.dias.ops.vector.Select(scan, BinaryOp.GT, 3, 6);
		ch.epfl.dias.ops.vector.Project prj = new ch.epfl.dias.ops.vector.Project(sel, new int[]{0,4});

		long startTime = System.nanoTime();
		prj.open();
		DBColumn[] result = prj.next();
		System.out.println(result[0]);
		long endTime = System.nanoTime();
		return (endTime - startTime);
	}

	static public long queryThreeVec(ColumnStore lineItem){
		// SELECT COUNT (*) FROM LINE_ITEM
		ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(lineItem,5000);
		ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(scan, Aggregate.COUNT, DataType.INT, 3);

		long startTime = System.nanoTime();
		agg.open();
		DBColumn[] result = agg.next();
		System.out.println(result[0]);
		long endTime = System.nanoTime();
		return (endTime - startTime);
	}

	static public long queryFourVec(ColumnStore lineItem){
		// Select SELECT L_ORDERKEY, L_quantity FROM LINE_ITEM WHERE col4 > 6
		ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(lineItem, 5000);
		ch.epfl.dias.ops.vector.Select sel = new ch.epfl.dias.ops.vector.Select(scan, BinaryOp.GE, 3, 6);
		ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(sel, Aggregate.AVG, DataType.DOUBLE, 4);

		long startTime = System.nanoTime();
		agg.open();
		DBColumn[] result = agg.next();
		System.out.println(result[0]);
		long endTime = System.nanoTime();
		return (endTime - startTime);
	}

	static public long queryFiveVec(ColumnStore lineItem, ColumnStore orders){
		// SELECT COUNT(*) FROM order JOIN lineitem ON (o_orderkey = orderkey)

		ch.epfl.dias.ops.vector.Scan scanOrder = new ch.epfl.dias.ops.vector.Scan(orders, 5000);
		ch.epfl.dias.ops.vector.Scan scanLineitem = new ch.epfl.dias.ops.vector.Scan(lineItem, 5000);

		ch.epfl.dias.ops.vector.Join join = new ch.epfl.dias.ops.vector.Join(scanLineitem, scanOrder, 0, 0);
		ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(join, Aggregate.COUNT, DataType.INT, 0);

		long startTime = System.nanoTime();
		agg.open();
		DBColumn[] result = agg.next();
		System.out.println(result[0]);
		long endTime = System.nanoTime();
		return (endTime - startTime);
	}

}
