/**
 * Created by eoin on 19/04/16.
 */
/*  jQuery ready function. Specify a function to execute when the DOM is fully loaded.  */
$(document).ready(
    /* This is the function that will get executed after the DOM is fully loaded */
    function () {
        $("#datepicker").datepicker({
            changeMonth: true,//this option for allowing user to select month
            changeYear: true //this option for allowing user to select from year range
        });
    }
);