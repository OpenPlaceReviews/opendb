var METRIC_STAB = function () {
    return {
        loadMetricsData: function() {
            $.getJSON("/api/metrics", function (data) {
                metricsData = data.metrics;
                setMetricsDataToTable();
            });
        },
        onReady: function() {
            $("#metrics-all").click(function() {
                setMetricsDataToTable();
            });
            $("#metrics-a").click(function() {
                setMetricsDataToTable();
            });
            $("#metrics-b").click(function() {
                setMetricsDataToTable();
            });
            $("#reset-metrics-b").click(function(){
                $.post("/api/metrics-reset?cnt=2", {})
                    .done(function(data){  metricsData = data.metrics; $("#metrics-b").prop("checked", true); setMetricsDataToTable(); })
                    .fail(function(xhr, status, error){  $("#result").html("ERROR: " + error); loadData(); });
            });

            $("#refresh-metrics").click(function(){
                $.get("/api/metrics", {})
                    .done(function(data){  metricsData = data.metrics; setMetricsDataToTable(); })
                    .fail(function(xhr, status, error){  $("#result").html("ERROR: " + error); loadData(); });
            });

            $("#reset-metrics-a").click(function(){
                $.post("/api/metrics-reset?cnt=1", {})
                    .done(function(data){  metricsData = data.metrics; $("#metrics-a").prop("checked", true); setMetricsDataToTable(); })
                    .fail(function(xhr, status, error){  $("#result").html("ERROR: " + error); loadData(); });
            });
        }
    };

    function setMetricsDataToTable() {
        var gid = 0;
        if ($("#metrics-a").prop("checked")) {
            gid = 1;
        }
        if ($("#metrics-b").prop("checked")) {
            gid = 2;
        }

        var table = $("#main-metrics-table");
        table.empty();
        var template = $("#metrics-template");

        for (var i = 0; i < metricsData.length; i++) {
            let item = metricsData[i];
            var newTemplate = template.clone()
                .appendTo(table)
                .show();
            var lid = item.id;
            if (lid.length > 50) {
                lid = lid.substring(0, 50);
            }
            newTemplate.find("[did='id']").html(lid);
            newTemplate.find("[did='count']").html(item.count[gid]);
            newTemplate.find("[did='total']").html(item.totalSec[gid]);
            newTemplate.find("[did='average']").html(item.avgMs[gid]);
            if (item.avgMs > 0) {
                newTemplate.find("[did='throughput']").html(Number(1000 / item.avgMs[gid]).toFixed(2));
            } else {
                newTemplate.find("[did='throughput']").html("-");
            }
        }
    }
} ();
