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
    public Connection link;	//connection object
    public void initconnection() throws Exception
    {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","119.3.167.84,117.78.2.224,119.3.249.45");  //hbase 服务地址
        configuration.set("hbase.zookeeper.property.clientPort","2181"); //端口号
        connection = ConnectionFactory.createConnection(configuration);
        admin = connection.getAdmin();

        link=connection;
    }
    public static void main(String[] args) throws Exception {
        opration o =new opration();
        o.initconnection();
        //o.createTable("user2");
        //o.truncateTable("user");
        //o.Put("user2");
        //o.find_family("user","1","follow");
        o.queryTable("typeScore2");
        //o.createsparkTable("typeScore");
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
    public void createsparkTable(String t) throws IOException
    {
        System.out.println("[hbaseoperation] start createtable...");
        String tableNameString = t;
        TableName tableName = TableName.valueOf(tableNameString);
        HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
        hTableDescriptor.addFamily(new HColumnDescriptor("score"));
        admin.createTable(hTableDescriptor);
        System.out.println("[hbaseoperation] end createtable...");
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
    public String create(String name) throws IOException //创建表
    {
        TableName table = TableName.valueOf(name);
        HTableDescriptor desc = new HTableDescriptor(table);
        desc.addFamily(new HColumnDescriptor("info"));
        desc.addFamily(new HColumnDescriptor("fan"));
        desc.addFamily(new HColumnDescriptor("attention"));
        admin.createTable(desc); //创建表
        return "success";
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
            put1.addColumn(Bytes.toBytes("base"), Bytes.toBytes("机器人"), Bytes.toBytes("机器人"+String.valueOf(i)));
            if(i<=10) {
                for(int j=1;j<=10;j++){
                    put1.addColumn(Bytes.toBytes("follow"), Bytes.toBytes(String.valueOf(i+j)), Bytes.toBytes("机器人"+String.valueOf(i+j)));
                }
            }
            if(i<=20){
                for(int j=i-1;j>=1&&j>=i-10;j--){
                    put1.addColumn(Bytes.toBytes("fans"), Bytes.toBytes(String.valueOf(j)), Bytes.toBytes("机器人"+String.valueOf(j)));
                }
            }
            putList.add(put1);
            table.put(putList);
        }
        System.out.println("[hbaseoperation] end insert...");
    }

    public void insert(String name) throws IOException //初始化插入2000条数据
    {
        Table table = connection.getTable(TableName.valueOf(name));
        List<Put> list = new ArrayList<Put>();
        for(int i=1;i<=2000;i++){
            Put inser;
            inser = new Put(Bytes.toBytes(String.valueOf(i)));
            inser.addColumn(Bytes.toBytes("info"), Bytes.toBytes("机器人"), Bytes.toBytes("机器人"+String.valueOf(i)));
            for(int j=1;j<=20;j++){
                if(j!=i){
                    inser.addColumn(Bytes.toBytes("attention"), Bytes.toBytes(String.valueOf(j)), Bytes.toBytes("机器人"+String.valueOf(j)));
                    inser.addColumn(Bytes.toBytes("fan"), Bytes.toBytes(String.valueOf(j)), Bytes.toBytes("机器人"+String.valueOf(j)));
                }
            }
            list.add(inser);
            table.put(list);
        }
    }
    public void insertdata(String name,String row,String value,String key) throws IOException //插入一条数据
    {
        Table table = link.getTable(TableName.valueOf(name));
        List<Put> list = new ArrayList<Put>();
        Put put1;
        put1 = new Put(Bytes.toBytes(row));
        put1.addColumn(Bytes.toBytes(key), Bytes.toBytes(value), Bytes.toBytes("机器人"+value));
        list.add(put1);
        table.put(list);
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
    public List<client> query_p(String name,String row,String key) throws IOException //查询粉丝或者关注所有数据
    {
        List<client> list = new ArrayList<client>();
        Table table = link.getTable(TableName.valueOf(name));
        Get get = new Get(row.getBytes());
        Result rs = table.get(get);
        List<Cell> listCells = rs.listCells();
        for (Cell cell : listCells) {
            client client=getClient(cell);
            if (key.equals(client.getFamily())) list.add(client);
        }
        return list;
    }
    public client getClient(Cell cell){
        String row = Bytes.toString(CellUtil.cloneRow(cell));
        long t = cell.getTimestamp();
        String f = Bytes.toString(CellUtil.cloneFamily(cell));
        String q = Bytes.toString(CellUtil.cloneQualifier(cell));
        String v = Bytes.toString(CellUtil.cloneValue(cell));
        client client = new client(row, t, f, q, v);
        return client;
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
    public void remove(String name,String row,String key,String value) throws IOException //删除一条数据
    {
        HTable table=new HTable( configuration,name);
        Delete de = new Delete(Bytes.toBytes(row));
        de.deleteColumns(Bytes.toBytes(key), Bytes.toBytes(value));
        table.delete(de);
        table.close();
    }
    public void deletequalifier(String t,String id,String qualifier,String family) throws IOException
    {
        HTable table=new HTable( configuration,t);
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