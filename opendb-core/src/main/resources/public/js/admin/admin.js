var REGISTRATION = function () {
    return {
        access: false,
        activeProfile: new Map(),
        userAccount: [],
        onReady: function () {
            $("#new-user-pill").click(function () {
                let allpills = $("#account-pills").children();
                allpills.removeClass("active");
                $("#new-user-pill").addClass("active");
                $("#login-account-msg").removeClass("hidden");
                $("#signup-account-msg").addClass("hidden");
                $("#signup-app-msg").addClass("hidden");
                $("#signup-account").removeClass("hidden");
                $("#login-account").addClass("hidden");
                $("#login-app").addClass("hidden");

            });

            $("#account-login-pill").click(function () {
                let allpills = $("#account-pills").children();
                allpills.removeClass("active");
                $("#account-login-pill").addClass("active");
                $("#login-account-msg").addClass("hidden");
                $("#signup-account-msg").removeClass("hidden");
                $("#signup-app-msg").addClass("hidden");
                $("#signup-account").addClass("hidden");
                $("#login-account").removeClass("hidden");
                $("#login-app").addClass("hidden");
            });

            $("#app-login-pill").click(function () {
                let allpills = $("#account-pills").children();
                allpills.removeClass("active");
                $("#app-login-pill").addClass("active");
                $("#login-account-msg").addClass("hidden");
                $("#signup-account-msg").addClass("hidden");
                $("#signup-app-msg").removeClass("hidden");
                $("#signup-account").addClass("hidden");
                $("#login-account").addClass("hidden");
                $("#login-app").removeClass("hidden");
            });

            $("#signup-account").click(function () {
                var username = $("#account-username").val();
                var password = $("#account-password").val();
                var entry = new Map();
                entry.set("username", username);
                entry.set("pwd", password);

                var exist = false;
                for (var i = 0; i < REGISTRATION.userAccount.length; i++) {
                    var obj = REGISTRATION.userAccount[i];
                    if (obj.get("username") === username) {
                        exist = true;
                    }
                }
                if (!exist) {
                    REGISTRATION.userAccount.push(entry);
                    REGISTRATION.activeProfile = REGISTRATION.userAccount[REGISTRATION.userAccount.length - 1];
                    $("#account-exist").removeClass("hidden");
                    $("#login-account-div").addClass("hidden");
                    $("#username").html(username);
                    $("#logout").removeClass("hidden");
                    $("#app-name").html("");
                    SIGNUP_VIEW.loadUserTab();
                } else {
                    alert("User is already exist");
                }
            });

            $("#login-account").click(function () {
                var username = $("#account-username").val();
                var password = $("#account-password").val();
                var exist = false;
                for (var i = 0; i < REGISTRATION.userAccount.length; i++) {
                    var obj = REGISTRATION.userAccount[i];
                    if (obj.get("username") === username) {
                        exist = true;
                        REGISTRATION.activeProfile = obj;
                    }
                }
                if (exist) {
                    $("#account-exist").removeClass("hidden");
                    $("#login-account-div").addClass("hidden");
                    $("#username").html(username);
                    SIGNUP_VIEW.loadUserTab();
                    $("#logout").removeClass("hidden");
                    let allpills = $("#active-account-pills").children();
                    allpills.removeClass("active");
                    $("#active-account-update-details-pill").addClass("active");
                    var appName = REGISTRATION.activeProfile.get("app-name");
                    $("#app-name").html(": " + appName);
                } else {
                    alert("Profile is not exist!")
                }
            });

            $("#login-app").click(function () {

            });


            $("#access-application-pill").click(function () {
                let allpills = $("#active-account-pills").children();
                allpills.removeClass("active");
                $("#access-application-pill").addClass("active");
                $("#access-application-div").removeClass("hidden");
                $("#signup-panel").addClass("hidden");
                $("#login-panel").addClass("hidden");
                $("#update-account-details-div").addClass("hidden");
            });

            $("#active-account-signup-pill").click(function () {
                let allpills = $("#active-account-pills").children();
                allpills.removeClass("active");
                $("#active-account-signup-pill").addClass("active");
                $("#access-application-div").addClass("hidden");
                $("#signup-panel").removeClass("hidden");
                $("#login-panel").addClass("hidden");
                $("#update-account-details-div").addClass("hidden");
            });

            $("#active-account-login-pill").click(function () {
                let allpills = $("#active-account-pills").children();
                allpills.removeClass("active");
                $("#active-account-login-pill").addClass("active");
                $("#access-application-div").addClass("hidden");
                $("#signup-panel").addClass("hidden");
                $("#update-account-details-div").addClass("hidden");
                $("#login-panel").removeClass("hidden");
            });

            $("#add-app-access").click(function () {
                var username = $("#app-username").val();
                var key = $("#app-private-key").val();

                REGISTRATION.activeProfile.set("app-name", username);
                REGISTRATION.activeProfile.set("app-private-key", key);
                REGISTRATION.activeProfile.set("app-access-type", "app-access-prk");

                $("#app-name").html(": " + username);
                $("#active-account-update-account-pill").removeClass("hidden");
                $("#access-application-pill").addClass("hidden");
                $("#active-account-signup-pill").addClass("hidden");
                $("#active-account-login-pill").addClass("hidden");

                $("#access-application-div").addClass("hidden");
                $("#signup-panel").addClass("hidden");
                $("#login-panel").addClass("hidden");

                let allpills = $("#active-account-pills").children();
                allpills.removeClass("active");
                $("#active-account-update-details-pill").addClass("active");
                $("#update-account-details-div").removeClass("hidden");
                $("#active-account-sign-operation-pill").removeClass("hidden");
                $("#active-account-add-operation-pill").removeClass("hidden");
                $("#sign-operation").addClass("hidden");
            });

            $("#active-account-update-details-pill").click(function () {
                let allpills = $("#active-account-pills").children();
                allpills.removeClass("active");
                $("#active-account-update-details-pill").addClass("active");
                $("#sign-message").addClass("hidden");
                $("#update-account-details-div").removeClass("hidden");
                $("#access-application-div").addClass("hidden");
                $("#login-panel").addClass("hidden");
                $("#signup-panel").addClass("hidden");
                $("#update-app-access-div").addClass("hidden");
                $("#sign-operation").addClass("hidden");
            });

            $("#active-account-update-account-pill").click(function () {
                let allpills = $("#active-account-pills").children();
                allpills.removeClass("active");
                $("#active-account-update-account-pill").addClass("active");
                $("#sign-message").addClass("hidden");
                $("#update-account-details-div").addClass("hidden");
                $("#update-app-access-div").removeClass("hidden");
                $("#sign-operation").addClass("hidden");
            });

            $("#active-account-sign-operation-pill").click(function () {
                let allpills = $("#active-account-pills").children();
                allpills.removeClass("active");
                $("#active-account-sign-operation-pill").addClass("active");
                $("#update-account-details-div").addClass("hidden");
                $("#update-app-access-div").addClass("hidden");
                $("#sign-name").val(REGISTRATION.activeProfile.get("app-name"));
                $("#sign-pk").val(REGISTRATION.activeProfile.get("app-private-key"));
                $("#sign-operation").removeClass("hidden");

                if (REGISTRATION.activeProfile.get("app-access-type") === "app-access-prk") {
                    document.getElementById('sign-message-private-key-radio').checked = true;
                    $("#sign-message-password-radio").prop('disabled', "disabled");
                    $('input[type=radio][name=sign-message]').change();
                }
            });

            $("#active-account-sign-message-pill").click(function () {
                let allpills = $("#active-account-pills").children();
                allpills.removeClass("active");
                $("#active-account-sign-message-pill").addClass("active");
                $("#update-account-details-div").addClass("hidden");
                $("#update-app-access-div").addClass("hidden");
                $("#sign-message").removeClass("hidden");
                $("#sign-name").val(REGISTRATION.activeProfile.get("app-name"));
                $("#sign-pk").val(REGISTRATION.activeProfile.get("app-private-key"));

                if (REGISTRATION.activeProfile.get("app-access-type") === "app-access-prk") {
                    document.getElementById('sign-message-private-key-radio').checked = true;
                    $("#sign-message-password-radio").prop('disabled', "disabled");
                    $('input[type=radio][name=sign-message]').change();
                } else if (REGISTRATION.activeProfile.get("app-access-type") === "app-access-pwd") {
                    document.getElementById('sign-message-password-radio').checked = true;
                    $("#sign-message-private-key-radio-radio").prop('disabled', "disabled");
                    $('input[type=radio][name=sign-message]').change();

                }

            });

            $("#update-account-password-btn").click(function () {
                var newPassword = $("#update-account-password-field").val();

                for (var i = 0; i < REGISTRATION.userAccount.length; i++) {
                    var obj = REGISTRATION.userAccount[i];
                    if (obj.get("username") === REGISTRATION.activeProfile.get("username")) {
                        obj.set("pwd", newPassword);
                        REGISTRATION.activeProfile = obj;
                    }
                }
            });

            $("#update-app-access-btn").click(function () {
                var rates = document.querySelector('input[name="updateAccess"]:checked').value;

                var username = $("#update-account-name").val();
                var currentObject;
                for (var i = 0; i < REGISTRATION.userAccount.length; i++) {
                    var obj = REGISTRATION.userAccount[i];
                    if (obj.get("username") === REGISTRATION.activeProfile.get("username")) {
                        currentObject = obj;
                    }
                }
                switch (rates) {
                    case 'password' : {
                        let key = $("#update-user-pwd").val();
                        obj.set("app-name", username);
                        obj.set("app-private-key", key);
                        obj.set("app-access-type", "app-access-pwd");
                        break;
                    }
                    case 'private-key' : {
                        let key = $("#update-account-prk").val();
                        obj.set("app-name", username);
                        obj.set("app-private-key", key);
                        obj.set("app-access-type", "app-access-prk");
                        break;
                    }
                    case 'oauth' : {
                        let oauthId = $("#update-oauth-id").val();
                        obj.set("app-name", username);
                        obj.set("app-private-key", oauthId);
                        obj.set("app-access-type", "app-access-oauth");
                        break;
                    }
                }

                $("#app-name").html(username);
                REGISTRATION.activeProfile = obj;
            });

            $("#logout").click(function () {
                REGISTRATION.activeProfile = new Map();
                // $("#login-account-div").removeClass("hidden");
                $("#account-exist").addClass("hidden");
                $("#logout").addClass("hidden");
                $("#sign-message").addClass("hidden");
                $("#sign-operation").addClass("hidden");

                SIGNUP_VIEW.loadUserTab()
            });

            $('input[type=radio][name=updateAccess]').change(function() {
                var rates = document.querySelector('input[name="updateAccess"]:checked').value;
                switch (rates) {
                    case 'password' : {
                        $("#new-password-access").removeClass("hidden");
                        $("#new-private-key-access").addClass("hidden");
                        $("#new-oauth-access").addClass("hidden");
                        break;
                    }
                    case 'private-key' : {
                        $("#new-password-access").addClass("hidden");
                        $("#new-private-key-access").removeClass("hidden");
                        $("#new-oauth-access").addClass("hidden");
                        break;
                    }
                    case 'oauth' : {
                        $("#new-password-access").addClass("hidden");
                        $("#new-private-key-access").addClass("hidden");
                        $("#new-oauth-access").removeClass("hidden");
                        break;
                    }
                }
            });
        }
    }
}();
var SIGNUP_VIEW = function () {
    return {
        loadURLParams: function (url) {

        },
        loadUserTab: function() {
            if (REGISTRATION.activeProfile.size === 0) {
                $("#user-nav").addClass("hidden");
                $("#signup-panel").addClass("hidden");
                $("#account-info").addClass("hidden");
                $("#login-signup-account").removeClass("hidden");
                $("#login-account-div").removeClass("hidden");
            } else {
                $("#account-info").removeClass("hidden");
                $("#login-signup-account").addClass("hidden");
                $("#login-account-div").addClass("hidden");
                // $("#user-nav").removeClass("hidden");
                // $("#signup-panel").removeClass("hidden");
                // if (STATUS_VIEW.loginName === "") {
                //     $("#sign-pill").addClass("hidden");
                // } else {
                //     $("#sign-pill").removeClass("hidden");
                // }
            }
        },
        onReady: function () {
            $("#signup-pill").click(function () {
                let allpills = $("#user-pills").children();
                allpills.removeClass("active");
                $("#signup-pill").addClass("active");
                $("#signup-panel").removeClass("hidden");
                $("#login-panel").addClass("hidden");
                $("#sign-message").addClass("hidden");
                $("#signup-old-private-key-div").addClass("hidden");
                $("#signup-oauth-old-div").addClass("hidden");
                $("#old-signup-password-div").addClass("hidden");
                $("#signup-password-div").addClass("col-md-12");
                $("#signup-password-div").removeClass("col-md-6");
            });

            $("#login-pill").click(function () {
                let allpills = $("#user-pills").children();
                allpills.removeClass("active");
                $("#login-pill").addClass("active");
                $("#signup-panel").addClass("hidden");
                $("#login-panel").removeClass("hidden");
                $("#sign-message").addClass("hidden");
                $("#login-update-key").addClass("hidden");
                $("#login-key").removeClass("hidden");
                $("#login-signup-key").prop("placeholder", "Login private key");
            });

            $("#sign-pill").click(function () {
                let allpills = $("#user-pills").children();
                allpills.removeClass("active");
                $("#sign-pill").addClass("active");
                $("#signup-panel").addClass("hidden");
                $("#login-panel").addClass("hidden");
                if (STATUS_VIEW.loginName === "") {
                    $("#sign-message").addClass("hidden");
                } else {
                    $("#sign-message").removeClass("hidden");
                }
            });

            $("#update-user-pill").click(function () {
                let allpills = $("#user-pills").children();
                allpills.removeClass("active");
                $("#update-user-pill").addClass("active");
                $("#signup-panel").removeClass("hidden");
                $("#login-panel").addClass("hidden");
                $("#sign-message").addClass("hidden");
                $("#signup-old-private-key-div").removeClass("hidden");
                $("#signup-oauth-old-div").removeClass("hidden");
                $("#old-signup-password-div").removeClass("hidden");
                $("#signup-password-div").addClass("col-md-6");
            });

            $("#update-login-pill").click(function () {
                let allpills = $("#user-pills").children();
                allpills.removeClass("active");
                $("#update-login-pill").addClass("active");
                $("#signup-panel").addClass("hidden");
                $("#login-panel").removeClass("hidden");
                $("#sign-message").addClass("hidden");
                $("#login-update-key").removeClass("hidden");
                $("#login-key").addClass("hidden");
                $("#login-signup-key").prop("placeholder", "Login private key");
            });

            $('input[type=radio][name=signup]').change(function() {
                var rates = document.querySelector('input[name="signup"]:checked').value;
                switch (rates) {
                    case 'password' : {
                        $("#signup-password").removeClass("hidden");
                        $("#signup-private-key").addClass("hidden");
                        $("#signup-oauth").addClass("hidden");
                        break;
                    }
                    case 'private-key' : {
                        $("#signup-password").addClass("hidden");
                        $("#signup-private-key").removeClass("hidden");
                        $("#signup-oauth").addClass("hidden");
                        break;
                    }
                    case 'oauth' : {
                        $("#signup-password").addClass("hidden");
                        $("#signup-private-key").addClass("hidden");
                        $("#signup-oauth").removeClass("hidden");
                        break;
                    }
                }
            });

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
                postActionWithDataUpdating("/api/auth/signup", obj, true);
            });

            $('input[type=radio][name=sign-message]').change(function() {
                var rates = document.querySelector('input[name="sign-message"]:checked').value;
                switch (rates) {
                    case 'password' : {
                        $("#sign-message-password").removeClass("hidden");
                        $("#sign-message-private-key").addClass("hidden");
                        break;
                    }
                    case 'private-key' : {
                        $("#sign-message-password").addClass("hidden");
                        $("#sign-message-private-key").removeClass("hidden");
                        break;
                    }
                }
            });

            $("#sign-btn").click(function () {
                var obj = {
                    "name": $("#sign-name").val(),
                    "pwd": $("#sign-pwd").val(),
                    "privateKey": $("#sign-pk").val(),
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
                    .done(function (data) {
                        $("#show-sign-json").removeClass("hidden");
                        $("#json-info").html(JSON.stringify(JSON.parse(data), null, 4));
                        done(data, true);
                    })
                    .fail(function (xhr, status, error) { fail(error, true); });
            });

            $("#add-op-btn").click(function () {
                var obj = {
                    "addToQueue":true,
                    "dontSignByServer":true
                };
                var params = $.param(obj);
                var json = $("#json-op").val();
                $.ajax({
                    url: '/api/auth/process-operation?' + params,
                    type: 'POST',
                    data: json,
                    contentType: 'application/json; charset=utf-8'
                })
                    .done(function (data) {done(data, true);})
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

            $('input[type=radio][name=login]').change(function() {
                var rates = document.querySelector('input[name="login"]:checked').value;
                switch (rates) {
                    case 'password' : {
                        $("#login-password").removeClass("hidden");
                        $("#login-private-key").addClass("hidden");
                        $("#login-oauth").addClass("hidden");
                        break;
                    }
                    case 'private-key' : {
                        $("#login-password").addClass("hidden");
                        $("#login-private-key").removeClass("hidden");
                        $("#login-oauth").addClass("hidden");
                        break;
                    }
                    case 'oauth' : {
                        $("#login-password").addClass("hidden");
                        $("#login-private-key").addClass("hidden");
                        $("#login-oauth").removeClass("hidden");
                        break;
                    }
                }
            });

            $("#login-btn").click(function () {
                var obj = {
                    "name": $("#login-name").val() + ':' + $("#purpose-name").val(),
                    "pwd": $("#login-pwd").val(),
                    "edit": "true",
                    "signupPrivateKey": $("#login-signup-key").val(),
                    "oauthProvider": $("#login-oauth-p").val(),
                    "oauthId": $("#login-oauth-id").val(),
                    "loginPubKey": $("#login-public-key").val(),
                    "loginAlgo": "EC",
                    "userDetails": $("#login-details").val()
                };
                postActionWithDataUpdating("/api/auth/login", obj, true);
            });
        }
    }
}();

var ERRORS_VIEW = function () {
    return {
        loadURLParams: function (url) {
            
        },

        loadErrorsData: function () {
            getJsonAction("/api/logs", function (data) {
                var items = "";
                var errs = 0;
                var templateItem = $("#errors-list-item");
                for (var i = data.logs.length - 1; i >= 0; i--) {
                    let op = data.logs[i];
                    var it = templateItem.clone();

                    if (op.status) {
                        it.find("[did='status']").html(op.status === "" ? "Error" : "Error: ");
                        it.find("[did='status-text']").html(op.status);
                        it.find("[did='object-name']").html("Failure object");
                    } else {
                        it.find("[did='status']").html("Success");
                    }
                    it.find("[did='log-message']").html(op.message);
                    it.find("[did='log-time']").html(new Date(op.utcTime).toUTCString());

                    if (op.status) {
                        errs++;
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
                    if (op.obj) {
                        it.find("[did='object-json']").html(JSON.stringify(op.obj, null, 4));
                    } else {
                        it.find("[did='obj-json-hidden']").prop("hidden", true);
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
            if (data.statusDescription !== "") {
                $("#status-description").html(" : " + data.statusDescription);
            } else {
                $("#status-description").html("");
            }
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
            $("#blocks-tab").html("Blocks (" + data.amountBlocks + ")");
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
            $("#refresh-admin-btn").click(function() {
                loadData();
            });

            $("#remove-orphaned-blocks-btn").click(function () {
                postActionWithDataUpdating("/api/mgmt/delete-orphaned-blocks", {
                    "blockListOrSingleValue": $("#remove-orphaned-blocks-txt").val()
                }, true);
            });
            $("#remove-queue-ops-btn").click(function () {
                postActionWithDataUpdating("/api/mgmt/delete-queue-ops", {
                    "opsListOrSingleValue": $("#remove-queue-ops-txt").val()
                }, true);
            });
            $("#admin-login-btn").click(function () {
                var obj = {
                    "pwd": $("#admin-login-key").val(),
                    "name": $("#admin-login-name").val()
                };
                postActionWithDataUpdating("/api/auth/admin-login", obj, true);
            });
            $("#home-tab").click(function () {
                window.history.pushState(null, "State Home", '/api/admin?view=home');
            });
            $("#blocks-tab").click(function () {
                window.history.pushState(null, "State Objects", '/api/admin?view=blocks');
            });
            $("#operations-tab").click(function () {
                window.history.pushState(null, "State Operations", '/api/admin?view=operations');
            });
            $("#objects-tab").click(function () {
                window.history.pushState(null, "State Objects", '/api/admin?view=objects');
            });
            $("#errors-tab").click(function () {
                window.history.pushState(null, "State Errors", '/api/admin?view=errors');
            });
            $("#api-tab").click(function () {
                window.history.pushState(null, "State API", '/api/admin?view=api');
            });
            $("#settings-tab").click(function () {
                window.history.pushState(null, "State Settings", '/api/admin?view=settings');
            });
            $("#metrics-tab").click(function () {
                window.history.pushState(null, "State Metrics", '/api/admin?view=metrics');
            });
            $("#ipfs-tab").click(function () {
                window.history.pushState(null, "State IPFS", '/api/admin?view=ipfs');
            });
            $("#reg-tab").click(function () {
                window.history.pushState(null, "State Reg", '/api/admin?view=users');
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

function postHandler(method, update) {
    return method
        .done(function (data) { done(data, update); })
        .fail(function (xhr, status, error) { fail(xhr.responseText, update); });
}

function handlerWithParams(method, functionDone, functionFail) {
    return method
        .done(function (data) {
            functionDone(data);
        })
        .fail(function (xhr, status, error) {
            functionFail(xhr.responseText);
        })
}

function postAction(url, obj, update) {
    return function() {
        postHandler($.post(url, obj ? obj : {}, update))
    }
}
function postActionWithoutFailParam(url, obj, functionDone, update) {
    return handlerWithParams($.post(url, obj), functionDone,  function (error) {
        fail(error, update);
    });
}
function postActionWithDataUpdating(url, obj, update) {
    return postHandler($.post(url, obj), update);
}
function getAction(url, obj, functionDone, update) {
    return handlerWithParams($.get(url, obj), functionDone, function (error) {
        fail(error, update);
    });
}
function getJsonAction(url, functionDone, obj) {
    return $.getJSON(url, obj ? obj : {}, function (data) {
        functionDone(data);
    });
}

function hideAlert() {
    $("#alert-template").hide();
}

function done(data, update) {
    var alert = $("#alert-template");
    alert.show();
    alert.removeClass("alert-warning");
    alert.addClass("alert-success");

    if (typeof data !== 'object') {
        data = JSON.parse(data);
    }
    if (data.msg) {
        $("#alert-status").html(data.status + ": ");
        $("#result").html(data.msg);
    } else if (data.status) {
        $("#alert-status").html(data.status);
        $("#result").html("");
    } else {
        $("#alert-status").html("OK");
        $("#result").html("");
    }

    if (update === undefined || update) {
        loadData();
    }
}

function fail(error, update) {
    var alert = $("#alert-template");
    alert.show();
    alert.removeClass("alert-success");
    alert.addClass("alert-warning");

    if (error !== "") {
        var parseJson = JSON.parse(error);
        if (parseJson.msg) {
            $("#alert-status").html(parseJson.status + ": ");
            $("#result").html(parseJson.msg);
        } else {
            $("#alert-status").html(parseJson.status);
            $("#result").html("");
        }
    } else {
        $("#alert-status").html("Error");
        $("#result").html("");
    }


    if (update === undefined || update) {
        loadData();
    }
}

function loadData() {
    getJsonAction("/api/status", function (data) {
        STATUS_VIEW.processStatusData(data);
        updateTabVisibility();
        OBJECTS_VIEW.loadObjectTypes();
        SIGNUP_VIEW.loadUserTab();
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
        $("#reg-tab").show();
        $("#ipfs-tab").hide();
    }
}

function loadURLParams() {
    var url = new URL(window.location.href);
    var lhref = window.location.href.toLowerCase();
    if (lhref.indexOf('api/admin?view=home') > 1) {
        $(".nav-tabs a[href=\"#home\"]").tab('show');
    } else if (lhref.indexOf('api/admin?view=objects') > 1) {
        $(".nav-tabs a[href=\"#objects\"]").tab('show');
        OBJECTS_VIEW.loadURLParams(url);
    } else if (lhref.indexOf('api/admin?view=operations') > 1) {
        $(".nav-tabs a[href=\"#operations\"]").tab('show');
        OPERATION_VIEW.loadURLParams(url);
    } else if (lhref.indexOf('api/admin?view=blocks') > 1) {
        $(".nav-tabs a[href=\"#blocks\"]").tab('show');
        BLOCKS_VIEW.loadURLParams(url);
    } else if (lhref.indexOf('api/admin?view=errors') > 1) {
        $(".nav-tabs a[href=\"#errors\"]").tab('show');
        ERRORS_VIEW.loadURLParams(url);
    } else if (lhref.indexOf('api/admin?view=api') > 1) {
        $(".nav-tabs a[href=\"#api\"]").tab('show');
    } else if (lhref.indexOf('api/admin?view=settings') > 1) {
        $(".nav-tabs a[href=\"#settings\"]").tab('show');
    } else if (lhref.indexOf('api/admin?view=metrics') > 1) {
        $(".nav-tabs a[href=\"#metrics\"]").tab('show');
    } else if (lhref.indexOf('api/admin?view=ipfs') > 1) {
        $(".nav-tabs a[href=\"#ipfs\"]").tab('show');
    } else if (lhref.indexOf('api/admin?view=users') > 1) {
        $(".nav-tabs a[href=\"#reg\"]").tab('show');
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
    REGISTRATION.onReady();

    // window.onpopstate = function (event) {
    //     if (!document.location.href.toString().includes("#popover")) {
    //         window.location.href = document.location;
    //     }
    // };

});
