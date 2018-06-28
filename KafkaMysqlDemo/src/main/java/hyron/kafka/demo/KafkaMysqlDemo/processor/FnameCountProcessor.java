package hyron.kafka.demo.KafkaMysqlDemo.processor;

import hyron.kafka.demo.KafkaMysqlDemo.bean.User;
import hyron.kafka.demo.SpringBootDemo.domain.MyObject;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueStore;

import java.sql.*;

public class FnameCountProcessor extends AbstractProcessor<String, MyObject> implements ProcessorSupplier<String, MyObject> {

    Connection conn = null;
    Statement stmt = null;
    String url = "jdbc:mysql://kafka-node0/test?user=root&password=mysql&useSSL=false&serverTimezone=Hongkong&useUnicode=true&characterEncoding=utf-8&allowPublicKeyRetrieval=true";

    private KeyValueStore<String, User> kvStore;

    @Override
    public Processor<String, MyObject> get() {
        return this;
    }

    @Override
    public void init(ProcessorContext context) {
        super.init(context);
        this.kvStore = (KeyValueStore<String, User>) context.getStateStore("user-fname-count");

        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection(url);
        }catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() {
        try {
            conn.close();
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public void process(String key, MyObject value) {

        if (key == null) {
            key = String.valueOf(value.getFname());
        }
        User user = this.kvStore.get(key);
        if (user == null) {
            user = new User(value.getFname(), 0);
        }

        System.out.println(user.getFcount());
        user.setFcount(user.getFcount() + 1);
        this.kvStore.put(key, user);
        // mysql
        insertUser(user);

        context().forward(key, value);
        context().commit();
    }

    private void insertUser(User user) {
        try {
            stmt = conn.createStatement();
            String sql = "INSERT INTO user_fcount(fname,fcount,ftime)VALUES('" + user.getFname() + "'," + user.getFcount() + ", now());";
            stmt.executeUpdate(sql);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                stmt.close();
                conn.commit();
            } catch (Exception ee){}
        }
    }
}
