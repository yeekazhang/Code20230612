package com.yeeka.hbase;

import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

@Data
public class HBaseUtil {

    private Connection connection;

    {
        try{
            Configuration conf = new Configuration();
            conf.addResource("hbase-site.xml");
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e){
            e.printStackTrace();
        }
    }

    public void closeConn() throws IOException{
        if (connection != null){
            connection.close();
        }
    }

    // 获取Table对象
    public Table getTable(String tableName) throws IOException{
        // isBlank 判断一个字符串是否 null,"","回车，空白字符"
        if (StringUtils.isBlank(tableName)){
            throw new RuntimeException("表名非法!");
        }
        Table table = connection.getTable(TableName.valueOf(tableName));
        return table;
    }

    // 定义put对象
    public Put createPut(String rowkey, String cf, String cq, String value){
        Put put = new Put(Bytes.toBytes(rowkey));
        return put.addColumn(Bytes.toBytes(cf),
                Bytes.toBytes(cq),
                Bytes.toBytes(value));
    }


    // 遍历扫描结果
    public void parseResult(Result result){

        // 获取一行中最原始的cell
        Cell[] cells = result.rawCells();

        // 遍历
        for (Cell cell : cells) {
            System.out.println("rowkey："+Bytes.toString(CellUtil.cloneRow(cell)));
            System.out.println("列名"+Bytes.toString(CellUtil.cloneFamily(cell))+ ":"+Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println("值:"+Bytes.toString(CellUtil.cloneValue(cell)));
        }
    }




}









