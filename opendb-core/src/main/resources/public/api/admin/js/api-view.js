var BOTS_VIEW = function () {
    function startStopBot(bot, action="start") {
        var obj = {
            "botName": bot
        };
        postActionWithDataUpdating("/api/bot/"+action, obj, false);
    }
    function enableDisableBot(bot, action="enable") {
        var obj = {
            "botName": bot,
            "interval": $("#bot-interval").val()
        };
        postActionWithDataUpdating("/api/bot/"+action, obj, false);
    }
    function fillTableBody(newTemplate, obj, logsColspan, update, botStat, opsColspan) {
        newTemplate.find("[did='time-start']").html(new Date(obj.timeStarted).toLocaleString('en-US'));
        if (obj.timeFinished) {
            newTemplate.find("[did='time-finish']").html(new Date(obj.timeFinished).toLocaleString('en-US'));
        }
        newTemplate.find("[did='amount-tasks']").html(obj.amountOfTasks);
        if (obj.running) {
            var progressBarValue = parseInt((botStat.progress / botStat.total) * 100);
            newTemplate.find("[did='progress-bar']")
                .attr("aria-valuenow", progressBarValue)
                .css("width",  progressBarValue + "%")
                .html(progressBarValue + "%");
        } else {
            newTemplate.find("[did='progress']").html("-");
        }
        if (obj.finishStatus) {
            newTemplate.find("[did='finish-status']").html(obj.finishStatus);
        } else if (obj.running) {
            newTemplate.find("[did='finish-status']").html("RUNNING");
        }
        newTemplate.find("[did='added-ops']").html();
        let opsCount = obj && obj.addedOperations? obj.addedOperations.length : 0;
        if (obj.addedOperations) {
            var ops = "";
            for (var l = 0; l < obj.addedOperations.length; l++) {
                var opObj = obj.addedOperations[l];
                var opInfo = " ( ";
                if (opObj.added > 0) {
                    opInfo += opObj.added + " added";
                }
                if (opObj.edited > 0) {
                    opInfo += opInfo === " ( " ? opObj.edited + " edited" : ", " + opObj.edited + " edited";
                }
                if (opObj.deleted > 0) {
                    opInfo += opInfo === " ( " ? opObj.deleted + " removed objects" : ", " + opObj.deleted + " removed objects";
                }
                opInfo += " )";
                ops += (l + 1) + ")" + "<a href='/api/admin?view=objects&browse=operation&key=" + opObj.hash + "'>" + opObj.hash + "</a><span>" + opInfo + "</span>" + "\n"
            }

            opsColspan.find("[did='logs-json']").html(ops);
        }
        if (obj.logEntries) {
            var logs = "";
            for (var k = 0; k < obj.logEntries.length; k++) {
                var logObj = obj.logEntries[k];
                logs += new Date(logObj.date).toLocaleString('en-US') + ": " + logObj.msg + "\n";
                if (logObj.exception) {
                    logs += logObj.exception + "\n";
                }
            }
            logsColspan.find("[did='logs-json']").html(logs);
        }
        var logsButton = newTemplate.find("[did='button-logs']");
        var operationsButton = newTemplate.find("[did='button-ops']");
        if (!update) {
            logsColspan.find("[did='logs']").addClass("hidden");
            opsColspan.find("[did='logs']").addClass("hidden");
        } else {
            operationsButton.html("Show " + opsCount);
            logsButton.html("Show");
        }
        if (logsColspan.find("[did='logs']").hasClass("hidden")) {
            logsButton.html("Show");
        } else {
            logsButton.html("Hide");
        }
        if (opsColspan.find("[did='logs']").hasClass("hidden")) {
            operationsButton.html("Show " + opsCount);
        } else {
            operationsButton.html("Hide");
        }

        if (!update) {
            logsButton.click(function () {
                showBotLogs(logsColspan, logsButton);
            });
            operationsButton.click(function () {
                showBotLogs(opsColspan, operationsButton, opsCount);
            })
        }
        function showBotLogs(colspan, button, opsCount) {
            if (colspan.find("[did='logs']").hasClass("hidden")) {
                colspan.find("[did='logs']").removeClass("hidden");
                button.html("Hide");
            } else {
                colspan.find("[did='logs']").addClass("hidden");
                if (opsCount) {
                    button.html("Show " + opsCount);
                } else {
                    button.html("Show");
                }
            }
        }
    }

    return {
        botStats: {},
        showBotHistory: function(bot, update) {
            var table = $("#main-bot-history-table");
            if (update) {
                getJsonAction("/api/bot", function (data) {
                    BOTS_VIEW.botStats = data;
                    var status = $('#main-bot-history-table > tr:first');
                    var ops = $('#main-bot-history-table > tr:eq(1)');
                    var logs = $('#main-bot-history-table > tr:eq(2)');
                    var botStat = BOTS_VIEW.botStats[bot];
                    fillTableBody(status, botStat.botRunStats[botStat.botRunStats.length - 1], logs, update, botStat, ops);
                });
            } else {
                var botStat = BOTS_VIEW.botStats[bot];
                table.empty();
                var template = $("#bot-history-template");
                var colspan_template = $("#bot-history-colspan-template");
                $("#bot-api").html(botStat.api);
                for (var i = botStat.botRunStats.length -1; i >= 0; i--) {
                    var obj = botStat.botRunStats[i];
                    var newTemplate = template.clone()
                        .appendTo(table)
                        .removeClass("hidden")
                        .show();
                    var ops = colspan_template.clone()
                        .appendTo(table)
                        .removeClass("hidden")
                        .show();
                    var logs = colspan_template.clone()
                        .appendTo(table)
                        .removeClass("hidden")
                        .show();
                    fillTableBody(newTemplate, obj, logs, update, botStat, ops);
                }
                $("#history-bot-name").val(bot);
                $("#bot-history-header").html("Bot stats for: " + bot);
            }
        },

        showBotScheduleSettings: function(bot) {
            var botState = BOTS_VIEW.botStats[bot];
            if (botState.settings.enabled) {
                $("#enable-bot-btn").addClass("hidden");
                $("#disable-bot-btn").removeClass("hidden");
                $("#update-bot-interval-btn").removeClass("hidden");
            } else {
                $("#disable-bot-btn").addClass("hidden");
                $("#update-bot-interval-btn").addClass("hidden");
                $("#enable-bot-btn").removeClass("hidden");
            }
            $("#bot-timeout-header").html("Setting schedule for bot: " + bot);
            $("#timeout-bot-name").val(bot);
            if (botState.settings.hasOwnProperty("interval_sec")) {
                $("#bot-interval").val(botState.settings.interval_sec);
            }
        },
        loadBotData: function () {
            getJsonAction("/api/bot", function (data) {
                BOTS_VIEW.botStats = data;
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
                                .css("width", progressBarValue + "%")
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

                    if (obj.settings && obj.settings.last_run) {
                        newTemplate.find("[did='last-launch']").html(new Date(obj.settings.last_run * 1000).toLocaleString());
                    } else {
                        newTemplate.find("[did='last-launch']").html("-");
                    }
                    if (!obj.systemBot) {
                        newTemplate.find("[did='bot-system']").addClass("hidden");
                    }
                    if(obj.settings && obj.settings.interval_sec && obj.settings.enabled) {
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
                            getJsonAction("/api/bot", function (data) {
                                BOTS_VIEW.botStats = data;
                                BOTS_VIEW.showBotHistory(obj.id, false);
                            });
                        });
                    newTemplate.find("[did='bot-schedule-btn']")
                        .click(function () {
                            BOTS_VIEW.showBotScheduleSettings(obj.id);
                        });
                    newTemplate.find("[did='actions']").html(action);
                }
            });
        },
        onReady: function () {
            $("#refresh-bot-table-btn").click(function () {
                BOTS_VIEW.loadBotData();
            });

            $("#refresh-bot-history-btn").click(function () {
                var botName = $(".modal-header #history-bot-name").val();
                BOTS_VIEW.showBotHistory(botName, true);
            });

            $("#enable-bot-btn").click(function () {
                enableDisableBot($("#timeout-bot-name").val(), "enable");
                $("#bot-timeout-modal .close").click();
                BOTS_VIEW.loadBotData();
            });

            $("#disable-bot-btn").click(function () {
                enableDisableBot($("#timeout-bot-name").val(), "disable");
                $("#bot-timeout-modal .close").click();
                BOTS_VIEW.loadBotData();
            });

            $("#update-bot-interval-btn").click(function () {
                var obj = {
                    "botName": $("#timeout-bot-name").val(),
                    "interval": $("#bot-interval").val()
                };
                $("#bot-timeout-modal .close").click();
                postActionWithDataUpdating("/api/bot/enable", obj, false);
                BOTS_VIEW.loadBotData();
            });
        }
    };
} ();


var API_VIEW = function () {

    return {
        endpoints: {},
        loadData: function () {
            getJsonAction("/api/endpoints", function (data) {
                API_VIEW.endpoints = data;
                var table = $("#main-api-table");
                table.empty();
                var template = $("#api-template");
                for (var key in data) {
                    let obj = data[key];
                    var newTemplate = template.clone()
                        .appendTo(table)
                        .removeClass("hidden")
                        .show();
                    newTemplate.find("[did='id']").
                        attr("href", '/api/public/'+obj.path+'/index').html(obj.id);
                    newTemplate.find("[did='path']").
                        attr("href", '/api/public/'+obj.path).html(obj.path);
                    var cacheSize = 0;
                    var cacheItems = 0;
                    for(var co in obj.cacheObjects) {
                        cacheSize += obj.cacheObjects[co].size;
                        cacheItems ++;
                    }
                    newTemplate.find("[did='cache']").html(
                        cacheItems + " items (" + (cacheSize / 1024).toFixed(1) + " KB)" );
                }
            });
        },
        onReady: function () {
            $("#refresh-api-table-btn").click(function () {
                API_VIEW.loadData();
            });
        }
    };
} ();