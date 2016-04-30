package pack;

import javax.servlet.jsp.JspException;
import javax.servlet.jsp.JspWriter;
import javax.servlet.jsp.tagext.SimpleTagSupport;
import java.io.IOException;
import java.io.StringWriter;

//this TagHandler manages data from the response created by the JSP
//and sends data from the backend to the JSP
public class HelloTag extends SimpleTagSupport {

    StringWriter sw = new StringWriter();
    private String sdate = null;
    private String edate = null;


    //setters and getters
    public void setSdate(String sdate) {
        this.sdate = sdate;

    }

    public String getEdate() {
        return edate;
    }

    public void setEdate(String edate) {
        this.edate = edate;

    }

    public void doTag()
            throws JspException, IOException {
        //executes once data from the JSP is sent
        if (sdate != "" && edate != "") {

            try {
                //pass data to the backend and run
                RippleMain rip = new RippleMain();
                String[] args = null;
                RippleMain.setStart(this.sdate);
                RippleMain.setEnd(this.edate);
                rip.main(args);

                JspWriter out = getJspContext().getOut();
                getJspContext().setAttribute("today", rip.getToday().substring(0, 9));
                out.println(this.sdate + " ,  " + this.getEdate());

            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
          /* use message from the body */
            getJspBody().invoke(sw);
            getJspContext().getOut().println(sw.toString());
        }
    }
}