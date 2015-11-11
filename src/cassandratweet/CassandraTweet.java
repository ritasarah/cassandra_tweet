/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package cassandratweet;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.DataType;
//import static com.datastax.driver.core.DataType.Name.UUID;
//import static com.datastax.driver.core.DataType.uuid;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.utils.UUIDs;
import java.util.Scanner;
import java.util.UUID;

/**
 *
 * @author ASUS
 */
public class CassandraTweet {

    public static Cluster cluster;
    public static Session session;
    
    public static void main(String[] args) {
        cluster = Cluster.builder().addContactPoint("167.205.35.19").build();
        session = cluster.connect("anda");
        
//        registerUser("user","user");
//        followFriend("user","user1");
//        tweet("user","tweet from users ya");
        showTweet("user");
        showTimeline("user");
                               
        // Clean up the connection by closing it
        cluster.close();
    }
    
    public static void run() {
        boolean stopper = false;
        String mode = "", username = "";
        Scanner input = new Scanner(System.in);
        
        while (!stopper) {
            mode = input.next().toLowerCase();
            if (mode.equals("/exit")) {
                stopper = true;
            }
            else if (mode.equals("/login")) {
                username = input.next().toLowerCase();
                String password = input.next().toLowerCase();
            }
            else if (mode.equals("/register")) {
                username = input.next().toLowerCase();
                String password = input.next().toLowerCase();
                registerUser(username, password);
            }
            else if (mode.equals("/follow")) {
                String friend_name = input.next().toLowerCase();
                followFriend (username, friend_name);
            }
            else if (mode.equals("/tweet_msg")) {
                String msg = input.nextLine().toLowerCase();
                tweet(username, msg);
            }
            else if (mode.equals("/tweet")) {
                String nickname = input.next().toLowerCase();
                showTweet(nickname);
            }
            else if (mode.equals("/timeline")) {
                String nickname = input.next().toLowerCase();
                showTimeline(nickname);
            }
        }
    }
    
    public static void printModeList() {
        System.out.println("Ketik '/login nickname password' untuk login ke akun Anda");
        System.out.println("Ketik '/register nickname password' untuk bergabung ke twitter");
        System.out.println("Ketik '/follow friend_name' untuk mem-follow teman Anda");
        System.out.println("Ketik '/tweet_msg pesan_Anda' untuk men-tweet pesan Anda");
        System.out.println("Ketik '/tweet username' untuk menampilkan tweet dari username tertentu");
        System.out.println("Ketik '/timeline username' untuk menampilkan timeline dari username tertentu");
        System.out.println("Ketik '/exit' untuk keluar dari program\n");
    }
    
    public static void registerUser(String uname,String pass){
        session.execute("INSERT INTO users (username, password) VALUES ('"+uname+"', '"+pass+"')");      
        System.out.println(uname+" registered");
    }

    public static void followFriend(String uname,String friend){
        session.execute("INSERT INTO friends (username, friend,since) VALUES ('"+uname+"', '"+friend+"',"+System.currentTimeMillis()+")");        
        session.execute("INSERT INTO followers (username, follower,since) VALUES ('"+uname+"', '"+friend+"',"+System.currentTimeMillis()+")");
        System.out.println(uname+" is now friends with "+friend);
    }

    public static void tweet(String uname,String tweet){
        String uuid = UUID.randomUUID().toString();
        UUID timeuuid = UUIDs.timeBased();
//        UUID.randomUUID().timestamp();
        
        session.execute("INSERT INTO tweets (tweet_id,username, body) VALUES ("+uuid+",'"+uname+"', '"+tweet+"')");
        session.execute("INSERT INTO timeline (username,time,tweet_id) VALUES ('"+uname+"',"+timeuuid+","+uuid+")");
        session.execute("INSERT INTO userline (username,time,tweet_id) VALUES ('"+uname+"',"+timeuuid+","+uuid+")");
        System.out.println(uname+":"+tweet+" published");
        //belum ketimeline semua follower
    }

    public static void showTweet(String uname){
        // Use select to get the user we just entered
        ResultSet results = session.execute("SELECT * FROM tweets WHERE username='"+uname+"'");
        for (Row row : results) {
            System.out.format(" %s : %s \n",  row.getString("username"), row.getString("body"));
        }
    }
    
    public static void showTimeline(String uname){
        // Use select to get the user we just entered
//        ResultSet resultstl = session.execute("SELECT * FROM tweets WHERE username='"+uname+"'");
        ResultSet resultstl = session.execute("SELECT * FROM timeline WHERE username='user'");
        System.out.println("hasil query select: " + resultstl.all().size());
        for (Row row : resultstl) {
//            System.out.format("%d\n", row.getInt("tweet_id"));
            System.out.format("%s : %s \n", row.getString("username"), row.getString("body"));
        }
        System.out.println("========================================masuk showTimeline========================================");

    }
}
