package hyron.kafka.demo.KafkaMysqlDemo.bean;

import java.util.Date;

public class User {

	private String fname;
	private int fcount;
	private Date ftime;

    public User() {
        this.ftime = new Date();
    }

	public User(String fname, int fcount) {
		this.fname = fname;
		this.fcount = fcount;
		this.ftime = new Date();
	}

	public String getFname() {
		return fname;
	}

	public void setFname(String fname) {
		this.fname = fname;
	}

	public int getFcount() {
		return fcount;
	}

	public void setFcount(int fcount) {
		this.fcount = fcount;
	}

	public Date getFtime() {
		return ftime;
	}

	public void setFtime(Date ftime) {
		this.ftime = ftime;
	}
}
