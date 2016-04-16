<%--
http://localhost:8080/Rippled/WEB-INF/Rippled.jsp
  Created by IntelliJ IDEA.
  User: eoin
  Date: 15/04/16
  Time: 20:25
  To change this template use File | Settings | File Templates.
--%>
<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<html>
<head>

    <link href='metricsgraphics.css' rel='stylesheet' type='text/css'>
    <script src='https://ajax.googleapis.com/ajax/libs/jquery/2.1.3/jquery.min.js'></script>
    <script src='https://cdnjs.cloudflare.com/ajax/libs/d3/3.5.0/d3.min.js' charset='utf-8'></script>
    <script src='metricsgraphics.js'></script>
    <!--<script src='../src/js/common/data_graphic.js'></script>-->

    <title>Rippled</title>

</head>
<style>
    .container {
        width: 90%;
        min-width: 960px;
    }
</style>
<body>
<%= "Hello World!" %>
<h3>Welcome Fool</h3>

<script>

    /*
     d3.json('data/ufo-sightings.json', function(data) {
     data = MG.convert.date(data, 'year');
     })*/
    jQuery.get('fake_users1.json', function (data) {
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
            y_accessor: 'value',

        });
    });
</script>
<div id='fake_users1' class='container'>
</div>

</body>
</html>
