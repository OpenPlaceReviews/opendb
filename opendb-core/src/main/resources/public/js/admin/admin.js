var SIGNUP_VIEW = function () {
    return {
        loadURLParams: function (url) {
            
        },

        onReady: function () {
            $("#signup-btn").click(function () {
                var obj = {
                    "pwd": $("#signup-pwd").val(),
                    "pwdOld": $("#signup-pwd-old").val(),
                    "name": $("#signup-name").val(),
                    "oauthProvider": $("#signup-oauth-p").val(),
                    "oauthProviderOld": $("#signup-oauth-p-old").val(),
                    "oauthId": $("#signup-oauth-id").val(),
                    "oauthIdOld": $("#signup-oauth-id-old").val(),
                    "algo": "EC",
                    "privateKey": $("#signup-user-prk").val(),
                    "privateKeyOld": $("#signup-user-prk-old").val(),
                    "publicKey": $("#signup-user-pbk").val(),
                    "publicKeyOld": $("#signup-user-pbk-old").val(),
                    "userDetails": $("#signup-details").val()
                };
                $.post("/api/auth/signup", obj)
                    .done(function (data) { done(data, true); })
                    .fail(function (xhr, status, error) { fail(error, true); });
            });

            $("#sign-btn").click(function () {
                var obj = {
                    "json": $("#sign-json").val(),
                    "name": $("#sign-name").val(),
                    "pwd": $("#sign-pwd").val(),
                    "privateKey": $("#sign-pk").val(),
                    "dontSignByServer": $("#sign-by-server").is(':checked')
                };
                $.post("/api/auth/process-operation", obj)
                    .done(function (data) { done(data, true); })
                    .fail(function (xhr, status, error) { fail(error, true); });
            });

            $("#sign-add-btn").click(function () {
                var obj = {
                    "name": $("#sign-name").val(),
                    "pwd": $("#sign-pwd").val(),
                    "privateKey": $("#sign-pk").val(),
                    "addToQueue": "true",
                    "dontSignByServer": $("#sign-by-server").is(':checked')
                };
                var params = $.param(obj);
                var json = $("#sign-json").val();
                $.ajax({
                    url: '/api/auth/process-operation?' + params,
                    type: 'POST',
                    data: json,
                    contentType: 'application/json; charset=utf-8'
                })
                    .done(function (data) { done(data, true); })
                    .fail(function (xhr, status, error) { fail(error, true); });
            });

            $("#login-btn").click(function () {
                var obj = {
                    "name": $("#login-name").val(),
                    "pwd": $("#login-pwd").val(),
                    "edit": "true",
                    "signupPrivateKey": $("#login-signup-key").val(),
                    "oauthProvider": $("#login-oauth-p").val(),
                    "oauthId": $("#login-oauth-id").val(),
                    "loginPubKey": $("#login-public-key").val(),
                    "loginAlgo": "EC",
                    "userDetails": $("#login-details").val()
                };
                $.post("/api/auth/login", obj)
                    .done(function (data) { done(data, true); })
                    .fail(function (xhr, status, error) { fail(error, true); });
            });
        }
    }
}();

var ERRORS_VIEW = function () {
    return {
        loadURLParams: function (url) {
            
        },

        loadErrorsData: function () {
            $.getJSON("/api/logs", function (data) {
                var items = "";
                var errs = 0;
                var templateItem = $("#errors-list-item");
                for (var i = data.logs.length - 1; i >= 0; i--) {
                    let op = data.logs[i];
                    var it = templateItem.clone();

                    it.find("[did='status']").html(op.status);
                    it.find("[did='log-message']").html(op.message);
                    it.find("[did='log-time']").html(new Date(op.utcTime).toUTCString());

                    if (op.status) {
                        errs++;
                    }
                    if (op.cause) {
                        it.find("[did='exception-message']").html(op.cause.detailMessage);
                    } else {
                        it.find("[did='excep-message-hidden']").prop("hidden", true);
                    }
                    if (op.block) {
                        it.find("[did='block']").html(op.block.block_id + " " + op.block.hash);
                    } else {
                        it.find("[did='block-hidden']").prop("hidden", true);
                    }
                    if (op.operation) {
                        it.find("[did='operation']").html(op.operation.type + " " + op.operation.hash);
                    } else {
                        it.find("[did='operation-hidden']").prop("hidden", true);
                    }
                    if (op.block) {
                        it.find("[did='block-json']").html(JSON.stringify(op.block, null, 4));
                    } else {
                        it.find("[did='block-json-hidden']").prop("hidden", true);
                    }
                    if (op.operation) {
                        it.find("[did='operation-json']").html(JSON.stringify(op.operation, null, 4));
                    } else {
                        it.find("[did='op-json-hidden']").prop("hidden", true);
                    }
                    if (op.cause) {
                        it.find("[did='full-exception-json']").html(JSON.stringify(op.cause, null, 4));
                    } else {
                        it.find("[did='full-json-hidden']").prop("hidden", true);
                    }
                    items += it.html();
                }
                $("#errors-tab").html("Logs (" + errs + " Errors)");
                $("#errors-list").html(items);
            });
        },

        onReady: function () {
        }
    }
}();

var STATUS_VIEW = function () {

    return {
        loginName: "",
        processStatusData: function (data) {
            var items = "";
            if(data.loginUser) {
                STATUS_VIEW.loginName = data.loginUser;
            } else {
                STATUS_VIEW.loginName = "";
            }
            $("#blockchain-status").html(data.status);
            items = "";
            for(var j = 0; j < data.sblocks.length; j++) {
                var url = "admin?view=operations&load=queue";
                if(j > 0) {
                    var bldescr = data.sblocks[j];
                    if(bldescr.startsWith("DB-")) {
                        bldescr = bldescr.substring(3);
                    }
                    var ind = bldescr.indexOf("-");
                    var hash = bldescr.substring(ind + 1);
                    var limit = parseInt(bldescr.substring(0, ind), 16);
                    url = "admin?view=blocks&search=to&hash="+hash+"&limit="+limit;
                }
                var name =  data.sblocks[j];
                if(name.length > 12) {
                    name = name.substring(0, 12);
                }
                items += "<li><a href='"+url+"'>" + name + "</a>";

            }
            $("#blockchain-blocks").html(items);
            $("#operations-tab").html("Operations (" + data.sblocks[0] + ")");

            $("#admin-user").html(data.serverUser);
            var ophS = Object.keys(data.orphanedBlocks);
            if(ophS.length > 0) {
                $("#blockchain-orphaned-blocks-header").show();
                $("#blockchain-orphaned-blocks").html(
                    JSON.stringify(ophS, null, 4) +
                    "<br><details><summary><b>Raw json</b></summary><pre>" +
                    JSON.stringify(data.orphanedBlocks, null, 4) + "</pre></details>");
            } else {
                $("#blockchain-orphaned-blocks-header").hide();
            }
        },
        loadURLParams: function (url) {
            
        },
        onReady: function () {
            $("#clear-list-btn").click(postAction("/api/mgmt/queue-clear"));
            $("#clear-log-btn").click(postAction("/api/mgmt/logs-clear"));
            $("#compact-btn").click(postAction("/api/mgmt/compact"));
            $("#replicate-btn").click(postAction("/api/mgmt/replicate"));
            $("#block-create-btn").click(postAction("/api/mgmt/create"));
            $("#block-revert-btn").click(postAction("/api/mgmt/revert-superblock"));
            $("#block-1revert-btn").click(postAction("/api/mgmt/revert-1-block"));
            $("#ops-pause-btn").click(postAction("/api/mgmt/toggle-blockchain-pause"));
            $("#blocks-pause-btn").click(postAction("/api/mgmt/toggle-blocks-pause"));
            $("#replicate-pause-btn").click(postAction("/api/mgmt/toggle-replicate-pause"));
            $("#block-bootstrap-btn").click(postAction("/api/mgmt/bootstrap"));

            $("#remove-orphaned-blocks-btn").click(function () {
                $.post("/api/mgmt/delete-orphaned-blocks", {
                    "blockListOrSingleValue": $("#remove-orphaned-blocks-txt").val()
                })
                    .done(function (data) { done(data, true); })
                    .fail(function (xhr, status, error) { fail(error, true); });
            });
            $("#remove-queue-ops-btn").click(function () {
                $.post("/api/mgmt/delete-queue-ops", {
                    "opsListOrSingleValue": $("#remove-queue-ops-txt").val()
                })
                    .done(function (data) { done(data, true); })
                    .fail(function (xhr, status, error) { fail(error, true); });
            });
            $("#admin-login-btn").click(function () {
                var obj = {
                    "pwd": $("#admin-login-key").val(),
                    "name": $("#admin-login-name").val()
                };
                $.post("/api/auth/admin-login", obj)
                    .done(function (data) { done(data, true); })
                    .fail(function (xhr, status, error) { fail(error, true); });
            });
        }
    }
}();

async function sha256(message) {
    // encode as UTF-8
    const msgBuffer = new TextEncoder('utf-8').encode(message);

    // hash the message
    const hashBuffer = await crypto.subtle.digest('SHA-256', msgBuffer);

    // convert ArrayBuffer to Array
    const hashArray = Array.from(new Uint8Array(hashBuffer));

    // convert bytes to hex string
    const hashHex = hashArray.map(b => ('00' + b.toString(16)).slice(-2)).join('');
    return hashHex;
}

function smallHash(hash) {
    var ind = hash.lastIndexOf(':');
    if (ind >= 0) {
        hash = hash.substring(ind + 1);
    }
    return hash.substring(0, 16);
}

function postAction(url) {
    return function() {
        $.post(url, {})
            .done(function (data) { done(data, true); })
            .fail(function (xhr, status, error) { fail(error, true); });
    }
}

var alertIsCreated = false;
function done(data, update) {
    var template = $("#layer-alert-success");
    var it = template.clone();

    it.removeClass("hidden");
    var container = $("#alert-container");
    console.log("html:" + container.html);
    console.log("length:" + container.length);

    if(!alertIsCreated && container.find(".alert").length === 0) {
        it.addClass("layer1");
        alertIsCreated = true;
    } else {
        it.addClass("layer2");
    }

    it.find("[did='result']").html(data);
    if (update) {
        loadData();
    }

    container.append(it);
}

function fail(error, update) {
    var template = $("#layer-alert-warn");
    var it = template.clone();

    it.removeClass("hidden");
    var container = $("#alert-container");
    console.log("html:" + container.html);
    console.log("length:" + container.length);

    if(!alertIsCreated && container.find(".alert").length === 0) {
        it.addClass("layer1");
        alertIsCreated = true;
    } else {
        it.addClass("layer2");
    }

    it.find("[did='result']").html(" ERROR: " + error);
    if (update) {
        loadData();
    }

    container.append(it);
}

function loadData() {
    $.getJSON("/api/status", function (data) {
        STATUS_VIEW.processStatusData(data);
        updateTabVisibility();
        OBJECTS_VIEW.loadObjectTypes();
        if (STATUS_VIEW.loginName != "") {
            METRIC_VIEW.loadMetricsData();
            API_VIEW.loadBotData();
            SETTINGS_VIEW.loadConfiguration();
            ERRORS_VIEW.loadErrorsData();
            IPFS_VIEW.loadIpfsStatusData();
        }
    });
}

function updateTabVisibility() {
    if (STATUS_VIEW.loginName != "") {
        $("#admin-login-user").html(STATUS_VIEW.loginName);
        $("#admin-actions").show();
        $("#owner-actions").show();
        $("#admin-login-div").hide();
        $("#reg").show();
        $("#errors-tab").show();
        $("#metrics-tab").show();
        $("#api-tab").show();
        $("#settings-tab").show();
        $("#reg-tab").show();
        $("#ipfs-tab").show();
    } else {
        $("#admin-actions").hide();
        $("#owner-actions").hide();
        $("#admin-login-div").show();
        $("#errors-tab").hide();
        $("#reg-tab").hide();
        $("#api-tab").hide();
        $("#settings-tab").hide();
        $("#metrics-tab").hide();
        $("#reg").hide();
        $("#ipfs-tab").hide();
    }
}

function loadURLParams() {
    var url = new URL(window.location.href);
    var lhref = window.location.href.toLowerCase();
    if (lhref.indexOf('api/admin?view=objects') > 1) {
        $(".nav-tabs a[href=\"#objects\"]").tab('show');
        OBJECTS_VIEW.loadURLParams(url);
    } else if (lhref.indexOf('api/admin?view=operations') > 1) {
        $(".nav-tabs a[href=\"#operations\"]").tab('show');
        OPERATION_VIEW.loadURLParams(url);
    } else if (lhref.indexOf('api/admin?view=blocks') > 1) {
        $(".nav-tabs a[href=\"#blocks\"]").tab('show');
        BLOCKS_VIEW.loadURLParams(url);
    }
}

$(document).ready(function () {
    updateTabVisibility();
    if (window.location.search !== "") {
        loadURLParams();
    }
    loadData();
    STATUS_VIEW.onReady();
    BLOCKS_VIEW.onReady();
    OPERATION_VIEW.onReady();
    OBJECTS_VIEW.onReady();
    API_VIEW.onReady();
    METRIC_VIEW.onReady();
    IPFS_VIEW.onReady();
    SETTINGS_VIEW.onReady();
    ERRORS_VIEW.onReady();
    SIGNUP_VIEW.onReady();

    // window.onpopstate = function (event) {
    //     if (!document.location.href.toString().includes("#popover")) {
    //         window.location.href = document.location;
    //     }
    // };

});
