var API_VIEW = function () {
    function startStopBot(bot, action="start") {
        var obj = {
            "botName": bot
        };
        // TODO std post handler
        $.post("/api/bot/"+action, obj)
            .done(function (data) {
                done(data, false);
            })
            .fail(function (xhr, status, error) {
                fail(error, false);
            });
    }
    function enableDisableBot(bot, action="enable") {
        var obj = {
            "botName": bot
        };
        // TODO std post handler
        $.post("/api/bot/"+action, obj)
            .done(function (data) {
                done(data, false);
            })
            .fail(function (xhr, status, error) {
                fail(error, false);
            });
    }


    return {
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
                            .removeClass("hidden")
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
                    fail(error, false);
                });
        },
        loadBotData: function () {
            $.getJSON("/api/bot", function (data) {
                var table = $("#main-bot-table");
                table.empty();
                var template = $("#bot-template");
                for (var key in data) {
                    let obj = data[key];

                    var newTemplate = template.clone()
                        .appendTo(table)
                        .removeClass("hidden")
                        .show();
                    newTemplate.find("[did='id']").html(obj.id);
                    var action = "";
                    newTemplate.find("[did='task-name']").html(obj.taskName);
                    newTemplate.find("[did='task-description']").html(obj.taskDescription);
                    if (obj.isRunning === false) {
                        newTemplate.find("[did='progress']").html("-");
                    } else {
                        var progressBarValue = parseInt((obj.progress / obj.total) * 100);
                        newTemplate.find("[did='progress-bar']")
                                .attr("aria-valuenow", progressBarValue)
                                .attr("style", "width:" + progressBarValue + "%")
                                .html(progressBarValue + "%");
                    }
                    if (obj.isRunning === false) {
                        newTemplate.find("[did='bot-start-btn']")
                            .removeClass("hidden")
                            .click(function () {
                                startStopBot(obj.id, "start");
                            });
                    } else {
                        newTemplate.find("[did='bot-stop-btn']")
                            .removeClass("hidden")
                            .click(function () {
                                startStopBot(obj.id, "stop");
                            });
                    }
                    newTemplate.find("[did='bot-schedule-btn']").click(function () {
                        //enableDisableBot(obj.id, "stop");
                    });

                    if (obj.settings && obj.settings.last_run) {
                        newTemplate.find("[did='last-launch']").html(new Date(obj.settings.last_run * 1000).toLocaleString());
                    } else {
                        newTemplate.find("[did='last-launch']").html("-");
                    }
                    if(obj.settings && obj.settings.interval_sec) {
                        var tm = obj.settings.interval_sec + " seconds";
                        if(obj.settings.interval_sec > 15 * 60) {
                            tm = (obj.settings.interval_sec / 60 ) + " minutes";
                        }
                        newTemplate.find("[did='interval']").html("every " + tm);
                    } else {
                        newTemplate.find("[did='interval']").html("-");
                    }
                    
                    newTemplate.find("[did='bot-show-history-btn']")
                        .click(function () {
                            API_VIEW.showBotHistory(obj.id);
                        });
                    newTemplate.find("[did='actions']").html(action);
                }
            });
        },
        onReady: function () {
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