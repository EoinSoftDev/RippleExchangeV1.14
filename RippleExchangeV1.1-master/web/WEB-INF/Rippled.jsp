<%@ taglib prefix="ex" uri="myTagLib" %>

<%--
http://localhost:8080/Rippled/WEB-INF/Rippled.jsp
  Created by IntelliJ IDEA.
  User: eoin
  Date: 15/04/16
  Time: 20:25
  To change this template use File | Settings | File Templates.
--%>
<%@ page contentType="text/html;charset=UTF-8" language="java" %>

/* if (request.getParameter("edate")!=null && request.getParameter("sdate")!=null)
{

}*/

/* if ( start != null && end != null) {
RippleMain runThis=new RippleMain();
runThis.setDates(start,end);
String[] args=null;
runThis.main(args);
}*/

<html lang="en">
<head>

    <link href="http://code.jquery.com/ui/1.10.4/themes/ui-lightness/jquery-ui.css">

    <link href='metricsgraphics.css' rel='stylesheet' type='text/css'>


    <script src='https://ajax.googleapis.com/ajax/libs/jquery/2.1.3/jquery.min.js'></script>
    <script src='https://cdnjs.cloudflare.com/ajax/libs/d3/3.5.0/d3.min.js' charset='utf-8'></script>
    <script src='metricsgraphics.js'></script>
    <!--<script src='../src/js/common/data_graphic.js'></script>-->
    <link rel="stylesheet" type="text/css" href="runnable.css">

    <title>Rippled</title>

</head>

<body>
<script src="//code.jquery.com/jquery-1.10.2.js"></script>
<script src="//code.jquery.com/ui/1.11.4/jquery-ui.js"></script>
<h3>Welcome Fool</h3>
<div style="height: 220px;" id="data_input">
    <form method="post">
        <p align="left">Enter Start Date: <input id="sdate" name="sdate" type="text" size="8" align="left">
            Enter End Date:<input id="edate" name="edate" type="text" size="8" align="right"></p>
        <br>
        <button class="large awesome blue" type="submit">Submit</button>
    </form>

    <script>
        //$(function() {
        $('#sdate').datepicker({dateFormat: 'yy-mm-dd'});
        $('#edate').datepicker({dateFormat: 'yy-mm-dd'});

        //  $( "#datepicker-13" ).datepicker("show");
        // });
    </script>
</div>

<script>

    /*
     d3.json('data/ufo-sightings.json', function(data) {
     data = MG.convert.date(data, 'year');
     })*/
    jQuery.get('data.json', function (data) {
        data = MG.convert.date(data, 'date');
        MG.data_graphic({
            title: "Line Chart",
            description: "This is a simple line chart. You can remove the area portion by adding area: false to the arguments list.",
            data: data,
            width: 600,
            height: 200,
            right: 40,
            target: document.getElementById('fake_users1'),
            x_accessor: 'date',
            y_accessor: 'payments',

        });
    });
</script>
<div id='fake_users1'>
</div>

<ex:Hello sdate="${param.sdate}" edate="${param.edate}"/>

</body>
</html>

