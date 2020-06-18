import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;


public class opration
{
    public Configuration configuration;
    public Connection connection;	//connection object
    public  Admin admin;			//operation object
    public void initconnection() throws Exception
    {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","119.3.167.84,117.78.2.224,119.3.249.45");  //hbase 服务地址
        configuration.set("hbase.zookeeper.property.clientPort","2181"); //端口号
        connection = ConnectionFactory.createConnection(configuration);
        admin = connection.getAdmin();
    }
    public static void main(String[] args) throws Exception {
        opration o =new opration();
        o.initconnection();
        //o.truncateTable("user");
        //o.Put("user");
    }
    public void AfollowB(String t,String A,String B) throws IOException {
        PutFamily(t,A,B,"follow");
        PutFamily(t,B,A,"fans");
    }
    public void ADeletefollowB(String t,String A,String B) throws IOException {
        deletequalifier(t,A,B,"follow");
        deletequalifier(t,B,A,"fans");
    }
    public Boolean AisfollowB(String t,String A,String B) throws IOException {
        List<User> list=find_family(t,A,"follow");
        for (User user :list){
            if(B.equals(user.getQualifier())) return true;
        }
        return false;
    }
    public void createTable(String t) throws IOException
    {
        System.out.println("[hbaseoperation] start createtable...");
        String tableNameString = t;
        TableName tableName = TableName.valueOf(tableNameString);
        HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
        hTableDescriptor.addFamily(new HColumnDescriptor("base"));
        hTableDescriptor.addFamily(new HColumnDescriptor("fans"));
        hTableDescriptor.addFamily(new HColumnDescriptor("follow"));
        admin.createTable(hTableDescriptor);
        System.out.println("[hbaseoperation] end createtable...");
    }
    public void Put(String t) throws IOException
    {
        System.out.println("[hbaseoperation] start insert...");

        Table table = connection.getTable(TableName.valueOf(t));
        List<Put> putList = new ArrayList<Put>();
        for(int i=1;i<=1000;i++){
            String id=String.valueOf(i);
            Put put1;
            put1 = new Put(Bytes.toBytes(String.valueOf(i)));
            put1.addColumn(Bytes.toBytes("base"), Bytes.toBytes("name"), Bytes.toBytes("name"+String.valueOf(i)));
            if(i<=10) {
                for(int j=1;j<=10;j++){
                    put1.addColumn(Bytes.toBytes("follow"), Bytes.toBytes(String.valueOf(i+j)), Bytes.toBytes("name"+String.valueOf(i+j)));
                }
            }
            if(i<=20){
                for(int j=i-1;j>=1&&j>=i-10;j--){
                    put1.addColumn(Bytes.toBytes("fans"), Bytes.toBytes(String.valueOf(j)), Bytes.toBytes("name"+String.valueOf(j)));
                }
            }
            putList.add(put1);
            table.put(putList);
        }
        System.out.println("[hbaseoperation] end insert...");
    }
    public void PutFamily(String t,String id,String qualifier,String family) throws IOException
    {
        System.out.println("[hbaseoperation] start insert...");

        Table table = connection.getTable(TableName.valueOf(t));
        List<Put> putList = new ArrayList<Put>();
        Put put1;
        put1 = new Put(Bytes.toBytes(id));
        put1.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes("name"+qualifier));
        putList.add(put1);
        table.put(putList);
        System.out.println("[hbaseoperation] end insert...");
    }
    public void Putpid(String t,String id,String pid) throws IOException
    {
        Table table = connection.getTable(TableName.valueOf(t));
        List<Put> putList = new ArrayList<Put>();
        Put put1;
        put1 = new Put(Bytes.toBytes(id));
        put1.addColumn(Bytes.toBytes("base"), Bytes.toBytes("pid"), Bytes.toBytes(pid));
        putList.add(put1);
        table.put(putList);
    }
    public List<User> findnopid(String t) throws IOException
    {
        List<User> list = new ArrayList<User>();
        System.out.println("find...");

        Table table = connection.getTable(TableName.valueOf(t));
        Filter filter = new SingleColumnValueFilter(Bytes.toBytes("base"), Bytes.toBytes("pid"),CompareOp.EQUAL, Bytes.toBytes("null"));
        Scan scan = new Scan();

        scan.setFilter(filter);

        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner)
        {
            List<Cell> listCells = result.listCells();
            for (Cell cell : listCells)
            {
                String rowKey = Bytes.toString(CellUtil.cloneRow(cell));
                long timestamp = cell.getTimestamp();
                String family = Bytes.toString(CellUtil.cloneFamily(cell));
                String qualifier  = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                User user=new User(rowKey,timestamp,family,qualifier,value);
                list.add(user);
                //System.out.println(" ===> rowKey : " + rowKey + ",  timestamp : " + timestamp + ", family : " + family + ", qualifier : " + qualifier + ", value : " + value);
            }
        }

        System.out.println("[hbaseoperation] end queryTableByCondition...");
        return list;
    }
    public List<User> find_family(String t,String id,String fami) throws IOException
    {
        List<User> list = new ArrayList<User>();
        System.out.println("find...");
        Table table = connection.getTable(TableName.valueOf(t));
        Get get = new Get(id.getBytes());
        Result result = table.get(get);

        List<Cell> listCells = result.listCells();
        for (Cell cell : listCells)
        {
            String rowKey = Bytes.toString(CellUtil.cloneRow(cell));
            long timestamp = cell.getTimestamp();
            String family = Bytes.toString(CellUtil.cloneFamily(cell));
            String qualifier	= Bytes.toString(CellUtil.cloneQualifier(cell));
            String value = Bytes.toString(CellUtil.cloneValue(cell));
            User user=new User(rowKey,timestamp,family,qualifier,value);
            if(family.equals(fami))  list.add(user);
            if(family.equals(fami)) System.out.println(" ===> rowKey : " + rowKey + ",  timestamp : " +  timestamp + ", family : " + family + ", qualifier : " + qualifier + ", value : " + value);
        }
        return list;
    }
    public void deletequalifier(String t,String id,String qualifier,String family) throws IOException
    {
        HTable table=new HTable( configuration,"user");
        Delete delete = new Delete(Bytes.toBytes(id));
        delete.deleteColumns(Bytes.toBytes(family), Bytes.toBytes(qualifier));
        table.delete(delete);
        table.close();
    }
    public void insert() throws IOException
    {
        System.out.println("[hbaseoperation] start insert...");

        Table table = connection.getTable(TableName.valueOf("table_book"));
        List<Put> putList = new ArrayList<Put>();

        Put put1;
        put1 = new Put(Bytes.toBytes("row1"));
        put1.addColumn(Bytes.toBytes("columnfamily_1"), Bytes.toBytes("name"), Bytes.toBytes("<<Java In Action>>"));
        put1.addColumn(Bytes.toBytes("columnfamily_1"), Bytes.toBytes("price"), Bytes.toBytes("98.50"));
        put1.addColumn(Bytes.toBytes("columnfamily_2"), Bytes.toBytes("author"), Bytes.toBytes("Tom"));
        put1.addColumn(Bytes.toBytes("columnfamily_2"), Bytes.toBytes("version"), Bytes.toBytes("3 thrd"));
        put1.addColumn(Bytes.toBytes("columnfamily_3"), Bytes.toBytes("discount"), Bytes.toBytes("5%"));

        Put put2;
        put2 = new Put(Bytes.toBytes("row2"));
        put2.addColumn(Bytes.toBytes("columnfamily_1"), Bytes.toBytes("name"), Bytes.toBytes("<<C++ Prime>>"));
        put2.addColumn(Bytes.toBytes("columnfamily_1"), Bytes.toBytes("price"), Bytes.toBytes("68.88"));
        put2.addColumn(Bytes.toBytes("columnfamily_2"), Bytes.toBytes("author"), Bytes.toBytes("Jimmy"));
        put2.addColumn(Bytes.toBytes("columnfamily_2"), Bytes.toBytes("version"), Bytes.toBytes("5 thrd"));
        put2.addColumn(Bytes.toBytes("columnfamily_3"), Bytes.toBytes("discount"), Bytes.toBytes("15%"));

        Put put3;
        put3 = new Put(Bytes.toBytes("row3"));
        put3.addColumn(Bytes.toBytes("columnfamily_1"), Bytes.toBytes("name"), Bytes.toBytes("<<Hadoop in Action>>"));
        put3.addColumn(Bytes.toBytes("columnfamily_1"), Bytes.toBytes("price"), Bytes.toBytes("78.92"));
        put3.addColumn(Bytes.toBytes("columnfamily_2"), Bytes.toBytes("author"), Bytes.toBytes("Kitty"));
        put3.addColumn(Bytes.toBytes("columnfamily_2"), Bytes.toBytes("version"), Bytes.toBytes("2 thrd"));
        put3.addColumn(Bytes.toBytes("columnfamily_3"), Bytes.toBytes("discount"), Bytes.toBytes("20%"));

        putList.add(put1);
        putList.add(put2);
        putList.add(put3);

        table.put(putList);

        System.out.println("[hbaseoperation] start insert...");
    }

    public void queryTable(String t) throws IOException
    {
        System.out.println("[hbaseoperation] start queryTable...");

        Table table = connection.getTable(TableName.valueOf(t));
        ResultScanner scanner = table.getScanner(new Scan());

        for (Result result : scanner)
        {
            byte[] row = result.getRow();
            System.out.println("row key is:" + Bytes.toString(row));

            List<Cell> listCells = result.listCells();
            for (Cell cell : listCells)
            {
                System.out.print("family:" + Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(),cell.getFamilyLength()));
                System.out.print("qualifier:" + Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()));
                System.out.print("value:" + Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                System.out.println("Timestamp:" + cell.getTimestamp());
            }
        }

        System.out.println("[hbaseoperation] end queryTable...");
    }

    public void queryTableByRowKey(String rowkey) throws IOException
    {
        System.out.println("[hbaseoperation] start queryTableByRowKey...");

        Table table = connection.getTable(TableName.valueOf("table_book"));
        Get get = new Get(rowkey.getBytes());
        Result result = table.get(get);

        List<Cell> listCells = result.listCells();
        for (Cell cell : listCells)
        {
            String rowKey = Bytes.toString(CellUtil.cloneRow(cell));
            long timestamp = cell.getTimestamp();
            String family = Bytes.toString(CellUtil.cloneFamily(cell));
            String qualifier	= Bytes.toString(CellUtil.cloneQualifier(cell));
            String value = Bytes.toString(CellUtil.cloneValue(cell));

            System.out.println(" ===> rowKey : " + rowKey + ",  timestamp : " +  timestamp + ", family : " + family + ", qualifier : " + qualifier + ", value : " + value);
        }

        System.out.println("[hbaseoperation] end queryTableByRowKey...");
    }


    public void queryTableByCondition(String authorName) throws IOException
    {
        System.out.println("[hbaseoperation] start queryTableByCondition...");

        Table table = connection.getTable(TableName.valueOf("table_book"));
        Filter filter = new SingleColumnValueFilter(Bytes.toBytes("columnfamily_2"), Bytes.toBytes("author"),CompareOp.EQUAL, Bytes.toBytes(authorName));
        Scan scan = new Scan();

        scan.setFilter(filter);

        ResultScanner scanner = table.getScanner(scan);

        for (Result result : scanner)
        {
            List<Cell> listCells = result.listCells();
            for (Cell cell : listCells)

            {
                String rowKey = Bytes.toString(CellUtil.cloneRow(cell));
                long timestamp = cell.getTimestamp();
                String family = Bytes.toString(CellUtil.cloneFamily(cell));
                String qualifier  = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));

                System.out.println(" ===> rowKey : " + rowKey + ",  timestamp : " + timestamp + ", family : " + family + ", qualifier : " + qualifier + ", value : " + value);
            }
        }

        System.out.println("[hbaseoperation] end queryTableByCondition...");
    }

    public void deleteColumnFamily(String cf) throws IOException
    {
        TableName tableName = TableName.valueOf("table_book");
        admin.deleteColumn(tableName, Bytes.toBytes(cf));
    }

    public void deleteByRowKey(String rowKey) throws IOException
    {
        Table table = connection.getTable(TableName.valueOf("table_book"));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        table.delete(delete);
        //queryTable();
    }

    public void truncateTable(String t) throws IOException
    {
        TableName tableName = TableName.valueOf(t);

        admin.disableTable(tableName);
        admin.truncateTable(tableName, true);
    }

    public void deleteTable() throws IOException
    {
        admin.disableTable(TableName.valueOf("table_book"));
        admin.deleteTable(TableName.valueOf("table_book"));
    }

}