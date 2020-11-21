package com.lyyq;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class StudentList {
    public static Configuration configuration;
    public static Connection conn;
    public static Admin admin;

    public static void creatTable(TableName tableName, String[] column) 
            throws IOException {
        if (admin.tableExists(tableName)) {
            System.out.println("table already exists");
        } 
        else {
            TableDescriptorBuilder tableDescriptor = TableDescriptorBuilder.newBuilder(tableName);
            for (String col : column) {
                ColumnFamilyDescriptor family = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(col)).build();
                tableDescriptor.setColumnFamily(family);
            }
            admin.createTable(tableDescriptor.build());
        }
    }

    public static void appendData(TableName tableName, String rowKey, String family, String column, String value) 
            throws IOException {
        Table table = conn.getTable(tableName);
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value));
        table.put(put);
        table.close();
        //.getBytes()
    }

    public static void scan(TableName tableName) 
            throws IOException {
        Table table = conn.getTable(tableName);
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        Iterator<Result> results = scanner.iterator();
        while (results.hasNext()){
            Result r = results.next();
            for (Cell cell : r.listCells()) {
                String column = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                System.out.println(column + "：" + value);
            }
        }
        /*Result result = scanner.next();
        while (result != null) {
            for (Cell cell : result.listCells()) {
                String column = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                System.out.println(column + "：" + value);
            }
            result = scanner.next();
        }*/
        scanner.close();
    }

    public static void getProvince(TableName tableName) 
            throws IOException{
        Table table = conn.getTable(tableName);
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        //Result result = scanner.next();
        scan.addColumn(Bytes.toBytes("Home"),Bytes.toBytes("Province"));
        Iterator<Result> results = scanner.iterator();
        while (results.hasNext()){
            Result r = results.next();
            for (Cell cell : r.listCells()) {
                String column = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                System.out.println(column + "：" + value);
            }
        }
        /*while (result != null) {
            for (Cell cell : result.listCells()) {
                String Column = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                System.out.println(Column + "：" + value);
            }
            result = scanner.next();
        }*/
    }

    public static void addFamily (TableName tableName, String newFamily)
            throws IOException{
        ColumnFamilyDescriptor family=ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(newFamily)).build();
        admin.disableTable(tableName);
        admin.addColumnFamily(tableName,family);
        admin.enableTable(tableName);
    }

    public static void dropTable (TableName tableName) 
            throws IOException{
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
    }

    public static void main(String[] args) 
            throws Exception {
        configuration = HBaseConfiguration.create();
        conn = ConnectionFactory.createConnection(configuration);
        admin = conn.getAdmin();

        TableName tableName = TableName.valueOf("studentslist");
        String[] column = {"ID", "Description", "Courses", "Home"};
        creatTable(tableName,column);
        appendData(tableName,"001","Description","Name","Li Lei");
        appendData(tableName,"002","Description","Name","Han Meimei");
        appendData(tableName,"003","Description","Name","Xiao Ming");

        appendData(tableName,"001","Description","Height","176");
        appendData(tableName,"002","Description","Height","183");
        appendData(tableName,"003","Description","Height","162");

        appendData(tableName,"001","Courses","Chinese","80");
        appendData(tableName,"002","Courses","Chinese","88");
        appendData(tableName,"003","Courses","Chinese","90");

        appendData(tableName,"001","Courses","Math","90");
        appendData(tableName,"002","Courses","Math","77");
        appendData(tableName,"003","Courses","Math","90");

        appendData(tableName,"001","Courses","Physics","95");
        appendData(tableName,"002","Courses","Physics","66");
        appendData(tableName,"003","Courses","Physics","90");

        appendData(tableName,"001","Home","Province","Zhejiang");
        appendData(tableName,"002","Home","Province","Beijing");
        appendData(tableName,"003","Home","Province","Shanghai");

        scan(tableName);
        getProvince(tableName);
        appendData(tableName,"001","Courses","English","95");
        appendData(tableName,"002","Courses","English","85");
        appendData(tableName,"003","Courses","English","98");

        addFamily(tableName,"Contact");
        appendData(tableName,"001","Contact","Email","lilei@qq.com");
        appendData(tableName,"002","Contact","Email","hanmeimei@qq.com");
        appendData(tableName,"003","Contact","Email","xiaoming@qq.com");

        scan(tableName);

        dropTable(tableName);
        admin.close();
        conn.close();
    }
}