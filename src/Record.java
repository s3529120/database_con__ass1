

import java.util.Date;

public class Record {
	private String bus_name;
	private String status;
	private Date reg_date;
	private Date cancel_date;
	private Date ren_date;
	private String form_state_num;
	private String pre_state_reg;
	private String abn;
	
	public Record(String bus_name, String status, Date reg_date, Date cancel_date, Date ren_date, String form_state_num,
			String pre_state_reg, String abn) {
		this.setBus_name(bus_name);
		this.setStatus(status);
		this.setReg_date(reg_date);
		this.setCancel_date(cancel_date);
		this.setRen_date(ren_date);
		this.setForm_state_num(form_state_num);
		this.setPre_state_reg(pre_state_reg);
		this.setAbn(abn);
	}

	public String getBus_name() {
		return bus_name;
	}

	public void setBus_name(String bus_name) {
		this.bus_name = bus_name;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public Date getReg_date() {
		return reg_date;
	}

	public void setReg_date(Date reg_date) {
		this.reg_date = reg_date;
	}

	public Date getCancel_date() {
		return cancel_date;
	}

	public void setCancel_date(Date cancel_date) {
		this.cancel_date = cancel_date;
	}

	public Date getRen_date() {
		return ren_date;
	}

	public void setRen_date(Date ren_date) {
		this.ren_date = ren_date;
	}

	public String getForm_state_num() {
		return form_state_num;
	}

	public void setForm_state_num(String form_state_num) {
		this.form_state_num = form_state_num;
	}

	public String getPre_state_reg() {
		return pre_state_reg;
	}

	public void setPre_state_reg(String pre_state_reg) {
		this.pre_state_reg = pre_state_reg;
	}

	public String getAbn() {
		return abn;
	}

	public void setAbn(String abn) {
		this.abn = abn;
	}
	
}
