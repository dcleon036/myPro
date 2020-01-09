package com.dc.mongo;

import com.mongodb.BasicDBObject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class MongoDBTest {
    static String url = "jdbc:oracle:thin:@10.71.21.151:1521:nftest";

    static String user = "risk30";

    static String password = "risk30_2019";

    static Connection conn = null;

    static MongoDatabase md = MongoDBUtil.getConnect();


    public static void main(String[] args) throws Exception {
        ExecutorService executorService = new ThreadPoolExecutor(Runtime.getRuntime().availableProcessors() ,
                Runtime.getRuntime().availableProcessors() + 1,
                60L, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(20000), new ThreadFactory() {
            private final AtomicInteger mCount = new AtomicInteger(1);
            @Override
            public Thread newThread(Runnable runnable) {
                return new Thread(runnable, "查询Mongodb" + mCount.getAndIncrement());
            }
        });
        //创建索引
        String tableName ="TI_STOCKS";
        MongoCollection<Document> collection = md.getCollection(tableName);
        //initConnect();
        createIndex(collection,tableName);
        queryData(collection);
        CompletableFuture<?>[] futures = new CompletableFuture[50];
        for(int i=0;i<futures.length ;i++){
            futures[i] = CompletableFuture.runAsync(()->{
                try {
                    queryData(collection);
                    //fillData(collection);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            },executorService);
        }
        //CompletableFuture.allOf(futures).join();
        //conn.close();
    }

    private static void queryData(MongoCollection<Document> collection) {
        Bson filter = null;
        FindIterable findIterable = null;
        MongoCursor cursor = null;
        for(int i = 0; i<257493 ;i++){
        filter = Filters.eq("C_STOCK_CODE", Integer.toString(i));
            findIterable = collection.find(filter);
            cursor = findIterable.iterator();
        while (cursor.hasNext()){
            System.out.println(cursor.next().toString());
        }
        }
    }


    /**
     * 创建索引
     * @param tableName
     * @throws SQLException
     */
    private static void createIndex(MongoCollection<Document> collection,String tableName) throws SQLException {
        List<BasicDBObject> bsons = new ArrayList<BasicDBObject>();
        bsons.add(new BasicDBObject().append("C_FUND_CODE", 1));//1升序，-1降序
        bsons.add(new BasicDBObject().append("C_STOCK_CODE", 1));//1升序，-1降序
        bsons.add(new BasicDBObject().append("N_POSITION_ID", 1));//1升序，-1降序
        for (BasicDBObject bson : bsons) {
            collection.createIndex(bson);
        }
//        String indexNamesql = "SELECT a.INDEX_NAME FROM user_indexes a WHERE a.TABLE_NAME = " + tableName;
//        String indexClumnNameSql = "SELECT TO_CHAR(WMSYS.WM_CONCAT(COLUMN_NAME)) as indexes" +
//                "            FROM USER_IND_COLUMNS" +
//                "           WHERE TABLE_NAME = " + tableName +
//                "             AND INDEX_NAME = ?" +
//                "           ORDER BY COLUMN_POSITION";

//        stmt = conn.prepareStatement(indexNamesql);
//        stmt.execute();
//        ResultSet resultSet = stmt.getResultSet() ;
//        ResultSet resultSet1 = null ;
//        while (resultSet.next()){
//            String INDEX_NAME = resultSet.getString("INDEX_NAME");
//            stmt = conn.prepareStatement(indexClumnNameSql);
//            stmt.execute();
//            resultSet1 = stmt.getResultSet();
//            while (resultSet1.next()){
//                resultSet1.getString("indexes");
//                List<BasicDBObject> bsons = new ArrayList<BasicDBObject>();
//                bsons.add(new BasicDBObject().append("account", 1));//1升序，-1降序
//                bsons.add(new BasicDBObject().append("ac_id", 1));//1升序，-1降序
//            }
//        }
    }

    /**
     * 查询数据放到MongoDB中
     */
    private static void fillData(MongoCollection<Document> collection) throws Exception {


         ResultSet resultSet =null;

         PreparedStatement stmt = null;
        try{
            String sql = "SELECT * FROM TI_STOCKS A";
            stmt = conn.prepareStatement(sql);
            stmt.execute();
            resultSet = stmt.getResultSet();
            ResultSetMetaData md = resultSet.getMetaData();//获取键名
            int columnCount = md.getColumnCount();//获取行的数量
            List<Document> list = new ArrayList<>();
            while(resultSet.next()){
                Document d = new Document();
                Map map = new HashMap<>();
                for(int i = 1; i < columnCount ;i++){
                    map.put(md.getColumnName(i),resultSet.getObject(i));
                }
                list.add(new Document(map));
            }
            collection.insertMany(list);
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            stmt.close();
            resultSet.close();
        }
    }

    /**
     * 数据库连接
     */
    private static void initConnect(){
        try{
            Class.forName("oracle.jdbc.OracleDriver");
            conn = DriverManager.getConnection(url, user, password);
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
