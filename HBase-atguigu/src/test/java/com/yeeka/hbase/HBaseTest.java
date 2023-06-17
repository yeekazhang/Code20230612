package com.yeeka.hbase;

import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;

public class HBaseTest {

    private HBaseUtil hbu = new HBaseUtil();

    @Test
    public void test() {
        System.out.println(hbu.getConnection());
    }

    @Test
    public void testPut() throws IOException {

        // 有一张表
        Table table = hbu.getTable("t1");

        // put一次就调用表一次put方法，或也可以批量操作
        ArrayList<Put> puts = new ArrayList<>();

        puts.add(hbu.createPut("a3", "f1", "name", "jack"));
        puts.add(hbu.createPut("a3", "f1", "age", "20"));
        puts.add(hbu.createPut("a3", "f1", "gender", "male"));

        // 批量插入多个cell
        table.put(puts);

        table.close();

    }

    @Test
    public void testGet() throws IOException {

        // 有一张表
        Table table = hbu.getTable("t1");

        // 封装一个Get对象，代表一次Get操作
        Get get = new Get(Bytes.toBytes("a3"));

        // 一行查询的结果
        Result result = table.get(get);

        hbu.parseResult(result);

        table.close();

    }


    // Scan
    @Test
    public void testScan() throws IOException {

        // 有一张表
        Table table = hbu.getTable("t1");

        // 封装一个Scan对象，代表一次Scan操作
        Scan scan = new Scan();

        scan.withStartRow(Bytes.toBytes("a1"));
        scan.withStopRow(Bytes.toBytes("z1"));

        // scanner是多行查询的结果
        ResultScanner scanner = table.getScanner(scan);

        for (Result result : scanner) {

            hbu.parseResult(result);

        }

        table.close();
    }

    @Test
    public void testDelete() throws IOException {

        // 有一张表
        Table table = hbu.getTable("t1");

        Delete delete = new Delete(Bytes.toBytes("a3"));

        // 删一列的最新版本，向指定的列添加一个cell(type=Delete, ts=最新的cell的ts)
        //delete.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("age"));

        // 删除这列的所有版本  向指定的列添加一个cell(type=DeleteColumn,ts=当前时间)
        //delete.addColumns(Bytes.toBytes("f1"), Bytes.toBytes("age"));

        // 删除列族的所有版本  向指定的行添加一个cell  f1:, timestamp=当前时间, type=DeleteFamily
        //delete.addFamily(Bytes.toBytes("f1"));

        // 删除一行的所有列族
        table.delete(delete);

        table.close();
    }
}










