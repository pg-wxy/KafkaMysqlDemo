package hyron.kafka.demo.SpringBootDemo.domain;

public class MyObject {
    private int user_id;
    private String fname;
    private String lname;

    public int getUser_id() {
        return user_id;
    }

    public void setUser_id(int user_id) {
        this.user_id = user_id;
    }

    public String getFname() {
        return fname;
    }

    public void setFname(String fname) {
        this.fname = fname;
    }

    public String getLname() {
        return lname;
    }

    public void setLname(String lname) {
        this.lname = lname;
    }

    @Override
    public String toString() {
        return "MyObject:(" +
                "user_id='" + user_id + '\'' +
                ", fname='" + fname + '\'' +
                ", lname='" + lname + '\'' +
                ')';
    }
}
