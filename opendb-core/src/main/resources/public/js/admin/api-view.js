var API_VIEW = function () {
    return {
        startBot: function(bot) {
            var obj = {
                "botName": bot
            };
            $.post("/api/bot/start", obj)
                .done(function (data) {
                    $("#result").html(data)
                })
                .fail(function (xhr, status, error) {
                    $("#result").html("ERROR: " + error);
                });
        },
        stopBot: function(bot) {
            var obj = {
                "botName": bot
            };
            $.post("/api/bot/stop", obj)
                .done(function (data) {
                    $("#result").html(data)
                })
                .fail(function (xhr, status, error) {
                    $("#result").html("ERROR: " + error);
                });
        },
        showBotHistory: function(bot) {
            var obj = {
                "botName": bot
            };
            $.getJSON("/api/bot/history", obj)
                .done(function (data) {
                    var table = $("#main-bot-history-table");
                    table.empty();
                    var template = $("#bot-history-template");
                    for (var i = 0; i < data.length; i++) {
                        var obj = data[i];
                        var newTemplate = template.clone()
                            .appendTo(table)
                            .show();
                        newTemplate.find("[did='bot-id']").html(obj.bot);
                        newTemplate.find("[did='start-date']").html(new Date(obj.startDate).toLocaleString());
                        if (obj.endDate !== undefined) {
                            newTemplate.find("[did='end-date']").html(new Date(obj.endDate).toLocaleString());
                        }
                        newTemplate.find("[did='total']").html(obj.total);
                        newTemplate.find("[did='processed']").html(obj.processed);
                        newTemplate.find("[did='status']").html(obj.status);
                    }
                    $("#history-bot-name").val(bot);
                    $("#bot-history-header").html("History of the launches for bot: " + bot);
                })
                .fail(function (xhr, status, error) {
                    $("#result").html("ERROR: " + error);
                });
        },
        loadBotData: function () {
            $.getJSON("/api/bot/stats", function (data) {
                var table = $("#main-bot-table");
                table.empty();
                var template = $("#bot-template");
                for (var key in data) {
                    var obj = data[key];

                    var newTemplate = template.clone()
                        .appendTo(table)
                        .show();
                    newTemplate.find("[did='id']").html(obj.id);
                    var action = "";
                    if ('botStats' in obj) {
                        newTemplate.find("[did='task-name']").html(obj.botStats.taskName);
                        newTemplate.find("[did='task-description']").html(obj.botStats.taskDescription);
                        if (!obj.botStats.isRunning) {
                            newTemplate.find("[did='progress']").html("NOT RUNNING");
                        } else {
                            var progressBarValue = parseInt((obj.botStats.progress / obj.botStats.total) * 100);
                            newTemplate.find("[did='progress']").html("<div class=\"progress\">\n" +
                                "  <div class=\"progress-bar progress-bar-info progress-bar-striped\" role=\"progressbar\"\n" +
                                "  aria-valuenow=\"" + progressBarValue + "\" aria-valuemin=\"0\" aria-valuemax=\"100\" style=\"width:" + progressBarValue + "%\">\n" + progressBarValue + "%" +
                                "  </div>\n" +
                                "</div>");
                        }
                        if (obj.botStats.isRunning === false) {
                            action += "<button type=\"button\" class=\"btn btn-primary\" style=\"margin-left:5px;\" onclick=\"API_VIEW.startBot('" + obj.id + "')\"><span class=\"glyphicon glyphicon-play\"></span></button>";
                        } else {
                            action += "<button type=\"button\" class=\"btn btn-primary\" style=\"margin-left:5px;\" onclick=\"API_VIEW.stopBot('" + obj.id + "')\"><span class=\"glyphicon glyphicon-pause\"></span></button>";
                        }
                    }
                    if (obj.started !== null && obj.started !== undefined) {
                        newTemplate.find("[did='last-launch']").html(new Date(obj.started).toLocaleString());
                    }
                    newTemplate.find("[did='interval']").html(obj.interval);

                    action += "<button type=\"button\" class=\"btn btn-primary\" data-toggle=\"modal\" data-target=\"#bot-history-modal\" style=\"margin-left:5px;\" onclick=\"API_VIEW.showBotHistory('" + obj.id + "')\"><span class=\"glyphicon glyphicon-eye-open\"></span></button>"
                    newTemplate.find("[did='actions']").html(action);
                }
            });
        },
        loadDBIndexData: function() {
            $.getJSON("/api/index", function (data) {
                var table = $("#main-index-table");
                table.empty();
                var infoTemplate = $("#db-info-template");
                var fullTemplate = $("#db-full-template");
                for (var key in data) {
                    var obj = data[key];
                    var newInfoTemplate = infoTemplate.clone()
                        .appendTo(table)
                        .show();
                    newInfoTemplate.find("[did='info-key']").html(key);
                    for (var keyObj in obj) {
                        var index = obj[keyObj];
                        var newFullTemplate = fullTemplate.clone()
                            .appendTo(table)
                            .show();
                        newFullTemplate.find("[did='index-id']").html(index.indexId);
                        newFullTemplate.find("[did='op-type']").html(index.opType);
                        newFullTemplate.find("[did='table-name']").html(index.columnDef.tableName);
                        newFullTemplate.find("[did='col-name']").html(index.columnDef.colName);
                        newFullTemplate.find("[did='col-type']").html(index.columnDef.colType);
                        newFullTemplate.find("[did='index']").html(index.columnDef.index);
                        if (index.fieldsExpression.length >= 1) {
                            newFullTemplate.find("[did='expression']").html(index.fieldsExpression[0].expression.toString());
                        }
                        newFullTemplate.find("[did='cache-runtime']").html(index.cacheRuntimeBlocks);
                        newFullTemplate.find("[did='cache-db']").html(index.cacheDBBlocks);
                    }
                }
            })
        },
        loadReportFiles: function() {
            // TODO
        },
        onReady: function () {
            $("#refresh-index-table-btn").click(function () {
                API_VIEW.loadDBIndexData();
            });

            $("#add-new-index-btn").click(function () {
                var myJSObject = {
                    tableName: $("#index-table-name").val(),
                    colName: $("#index-col-name").val(),
                    colType: $("#index-col-type").val(),
                    types: $("#index-op-types").val(),
                    index: $("#index-type").val(),
                    sqlMapping: $("#index-col-sql-mapping").val(),
                    cacheRuntimeMax: $("#index-cacheRuntimeMax").val(),
                    cacheDbIndex: $("#index-cacheDbIndex").val(),
                    field: ($("#index-field").val()).split(",")
                };
                $.ajax("/api/mgmt/index", {
                    data: JSON.stringify(myJSObject),
                    contentType: 'application/json',
                    type: 'POST',
                    success: function (data) {
                        $("#result").html("ADDED: " + data);
                        API_VIEW.loadDBIndexData();
                    },
                    error: function (xhr, status, error) {
                        $("#result").html("ERROR: " + error);
                    }
                });
                $("#new-index-modal .close").click();
            });

            $("#refresh-api-table-btn").click(function () {
                API_VIEW.loadReportFiles();
            });

            $("#refresh-bot-table-btn").click(function () {
                API_VIEW.loadBotData();
            });

            $("#refresh-bot-history-btn").click(function () {
                var botName = $(".modal-header #history-bot-name").val();
                API_VIEW.showBotHistory(botName);
            });
        }
    };
} ();