package com.example.flink.mysql;

import com.alibaba.druid.pool.DruidDataSource;
import com.example.flink.vo.ApisMessageVo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.UUID;

/**
 * Created by 86131 on 2020/1/10.
 */
public class SinkMysql extends RichSinkFunction<ApisMessageVo> {
    DruidDataSource druidDataSource;  //阿里的连接池
    private PreparedStatement ps;
    private Connection connection;


    /**
     * open() 方法中建立连接，这样不用每次 invoke 的时候都要建立连接和释放连接
     *
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        druidDataSource = new DruidDataSource();
        connection = getConnection(druidDataSource);  //连接池
    }

    @Override
    public void close() throws Exception {
        super.close();
        //关闭连接和释放资源
        if (connection != null) {
            connection.close();
        }
        if (ps != null) {
            ps.close();
        }
    }

    /**
     * 每条数据的插入都要调用一次 invoke() 方法
     *
     * @param value
     * @param context
     * @throws Exception
     */
    public void invoke(ApisMessageVo value, Context context) throws Exception {
        //获取对象，换成自己对象，或者什么string，随意。

        ps = connection.prepareStatement("insert into message_info(" +
                "ID , " +
                "communication_type ," +
                "communication_address ) values(?,?,?);");
        ps.setString(1, UUID.randomUUID().toString().replaceAll("-", ""));
        ps.setString(2, value.getCommunicationType());
        ps.setString(3, value.getCommunicationAddress());
        ps.executeUpdate(); //结果集
        System.out.println("成功");
    }

    private static Connection getConnection(DruidDataSource druidDataSource) {
        druidDataSource.setDriverClassName("com.mysql.jdbc.Driver");
        //注意，替换成自己本地的 mysql 数据库地址和用户名、密码
        druidDataSource.setUrl("jdbc:mysql://localhost:3306/wllpeter_demo");
        druidDataSource.setUsername("root");
        druidDataSource.setPassword("root");
        //设置连接池的一些参数
        //1.数据库连接池初始化的连接个数
        druidDataSource.setInitialSize(50);
        //2.指定最大的连接数，同一时刻可以同时向数据库申请的连接数
        druidDataSource.setMaxActive(200);
        //3.指定小连接数：在数据库连接池空闲状态下，连接池中保存的最少的空闲连接数
        druidDataSource.setMinIdle(30);
        Connection con = null;
        try {
            con = druidDataSource.getConnection();
            System.out.println("创建连接池：" + con);
        } catch (Exception e) {
            System.out.println("-----------mysql get connection has exception , msg = " + e.getMessage());
        }
        return con;
    }
}