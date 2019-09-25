    var loginName = "";
    // TODO move vars into objects i.e. METRICS_VIEW.metricsData etc...
    var metricsData = [];
    var globalObjectTypes = {};
    var globalConfig = {};


    ////////////////////// UTILITIES /////////////////////
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
        if(ind >= 0) {
            hash = hash.substring(ind + 1);
        }
        return hash.substring(0, 16);
    }
    //\\\\\\\\\\\\\\\\\\\\\\\\\\ UTILITIES \\\\\\\\\\\\\\\\\\\\\\

    ///////////////////////////// MAIN /////////////////////////////
    function loadData(checkUrl) {
        refreshUser();
        $.getJSON( "/api/auth/admin-status", function( data ) {
            loginName = data.admin;
            refreshUser();
            loadAllData(checkUrl);
        });
    }

    function loadAllData(checkUrl) {
        loadStatusData();
        OBJECTS_VIEW.loadObjectsData();
        $.getJSON("/api/blocks", function( data ) {
            BLOCKS_VIEW.processBlocksResult(data);
        });
        if (loginName != "") {
            METRIC_VIEW.loadMetricsData();
            // TODO make functions with same names i.e. loadData
            API_VIEW.loadBotData();
            API_VIEW.loadDBIndexData();
            API_VIEW.loadReportFiles();
            SETTINGS_VIEW.loadConfiguration();
            loadErrorsData();
            IPFS_VIEW.loadIpfsStatusData();
        }
        if (window.location.search !== "" && checkUrl) {
            checkUrlParam();
        }
    }

    // TODO move into status VIEW
    function refreshUser() {
        if(loginName != "") {
            $("#admin-login-user").html(loginName);
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

    function checkUrlParam() {
        // objects -> http://localhost:6463/api/admin?view=objects&filter=&search=osm.place&type=id&key=12345662&history=true
        // operations -> http://localhost:6463/api/admin?view=operations&loadBy=blockId&key=0
        // blocks -> http://localhost:6463/api/admin?view=blocks&search=from&hash=213&limit=123
        var url = new URL(window.location.href);
        // TODO move code into separate tabs to process
        if (window.location.href.toLowerCase().indexOf('api/admin?view=objects') > 1) {
            $(".nav-tabs a[href=\"#objects\"]").tab('show');
            var filter = url.searchParams.get('filter');
            if (filter !== null) {
                $("#filter-list").val(filter).change();
                if (filter === "operation") {
                    $("#name-key-field").text("Input operation Hash");
                } else {
                    $("#name-key-field").text("Input user Id");
                }
            }
            var searchTypeListValue = url.searchParams.get('search');
            if (searchTypeListValue !== null) {
                $("#type-list").val(searchTypeListValue);
                $("#type-list").change();
            }
            var typeSearch = url.searchParams.get('type');
            if (typeSearch !== null) {
                $("#search-type-list").val(typeSearch).change();
            }
            var checkboxValue = url.searchParams.get('history');
            if (checkboxValue !== null) {
                $("#historyCheckbox").prop('checked', checkboxValue === "true");
            } else {
                $("#search-type-list").change();
            }
            var limitValue = url.searchParams.get('limit');
            if (limitValue !== null) {
                $("#limit-field").val(limitValue);
            }
            var keyValue = url.searchParams.get('key');
            if (keyValue !== null) {
                $("#search-key").val(keyValue);
            }

            OBJECTS_VIEW.loadObjectView(false);
        } else if (window.location.href.toLowerCase().indexOf('api/admin?view=operations') > 1) {
            $(".nav-tabs a[href=\"#operations\"]").tab('show');
            var loadType = url.searchParams.get('loadBy');
            if (loadType !== null) {
                $("#operations-search").val(loadType).change();
            }
            var key = url.searchParams.get('key');
            if (key !== null) {
                $("#operations-key").val(key);
            }

            OPERATION_VIEW.loadOperationView();
        } else if (window.location.href.toLowerCase().indexOf('api/admin?view=blocks') > 1) {
            $(".nav-tabs a[href=\"#blocks\"]").tab('show');
            var searchType = url.searchParams.get('search');
            if (searchType !== null) {
                $("#blocks-search").val(searchType).change();
            }
            var hashValue = url.searchParams.get('hash');
            if (hashValue !== null) {
                $("#search-block-field").val(hashValue);
            }
            var limit = url.searchParams.get('limit');
            if (limit !== null) {
                $("#block-limit-value").val(limit);
            }

            BLOCKS_VIEW.loadBlockView();
        }
    }
    //\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\ MAIN \\\\\\\\\\\\\\\\\\\\\\\\\

    //////////////////////// FUNCTIONS TAB STATUS /////////////////////
    function loadStatusData() {
        $.getJSON( "/api/status", function( data ) {
            var items = "";
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

        });
    }
    //\\\\\\\\\\\\\\\\\ FUNCTIONS TAB STATUS \\\\\\\\\\\\\\\\\\\\\\\\\

    ///////////////////// FUNCTIONS TAB LOGS ////////////////////////
    // TODO MOVE to ERRORS_VIEW
    function loadErrorsData() {
        $.getJSON( "/api/logs", function( data ) {
            var items = "";
            var errs = 0;
            var templateItem = $("#errors-list-item");
            for(var i = 0; i < data.logs.length; i++)  {
                let op = data.logs[i];
                var it = templateItem.clone();

                it.find("[did='status']").html(op.status);
                it.find("[did='log-message']").html(op.message);
                it.find("[did='log-time']").html(new Date(op.utcTime).toUTCString());

                if(op.status) {
                    errs++;
                }
                if(op.cause) {
                    it.find("[did='exception-message']").html(op.cause.detailMessage);
                } else {
                    it.find("[did='excep-message-hidden']").prop("hidden", true);
                }
                if(op.block) {
                    it.find("[did='block']").html(op.block.block_id + " " + op.block.hash);
                } else {
                    it.find("[did='block-hidden']").prop("hidden", true);
                }
                if(op.operation) {
                    it.find("[did='operation']").html(op.operation.type + " " + op.operation.hash);
                } else {
                    it.find("[did='operation-hidden']").prop("hidden", true);
                }
                if(op.block) {
                    it.find("[did='block-json']").html(JSON.stringify(op.block, null, 4));
                } else {
                    it.find("[did='block-json-hidden']").prop("hidden", true);
                }
                if(op.operation) {
                    it.find("[did='operation-json']").html(JSON.stringify(op.operation, null, 4));
                } else {
                    it.find("[did='op-json-hidden']").prop("hidden", true);
                }
                if(op.cause) {
                    it.find("[did='full-exception-json']").html(JSON.stringify(op.cause, null, 4));
                } else {
                    it.find("[did='full-json-hidden']").prop("hidden", true);
                }
                items += it.html();
            }
            $("#errors-tab").html("Logs (" + errs + " Errors)");
            $("#errors-list").html(items);
        });
    }
    //\\\\\\\\\\\\\\\\\ FUNCTIONS TAB LOGS \\\\\\\\\\\\\\\\\\\\\\\\\

    var editor = {};
    var originObject;
    var globalObjects = {};
    $( document ).ready(function() {
        loadData(true);

        // TODO create method
        ////////////////////////// TAB STATUS //////////////////////
        $("#clear-list-btn").click(function(){
            $.post("/api/mgmt/queue-clear", {},  function(data, status){
                loadData();
            });
        });

        $("#clear-log-btn").click(function(){
            $.post("/api/mgmt/logs-clear", {},  function(data, status){
                loadData();
            });
        });

        $("#compact-btn").click(function(){
            $.post("/api/mgmt/compact", {},  function(data, status){
                loadData();
            });
        });

        $("#replicate-btn").click(function(){
            $.post("/api/mgmt/replicate", {},  function(data, status){
                loadData();
            });
        });

        $("#block-create-btn").click(function(){
            $.post("/api/mgmt/create", {})
                .done(function(data){  $("#result").html(data); loadData(); })
                .fail(function(xhr, status, error){  $("#result").html("ERROR: " + error); loadData(); });
        });

        $("#block-revert-btn").click(function(){
            $.post("/api/mgmt/revert-superblock", {})
                .done(function(data){  $("#result").html(data); loadData(); })
                .fail(function(xhr, status, error){  $("#result").html("ERROR: " + error); loadData(); });
        });
        $("#block-1revert-btn").click(function(){
            $.post("/api/mgmt/revert-1-block", {})
                .done(function(data){  $("#result").html(data); loadData(); })
                .fail(function(xhr, status, error){  $("#result").html("ERROR: " + error); loadData(); });
        });
        $("#ops-pause-btn").click(function(){
            $.post("/api/mgmt/toggle-blockchain-pause", {})
                .done(function(data){  $("#result").html(data); loadData(); })
                .fail(function(xhr, status, error){  $("#result").html("ERROR: " + error); loadData(); });
        });
        $("#blocks-pause-btn").click(function(){
            $.post("/api/mgmt/toggle-blocks-pause", {})
                .done(function(data){  $("#result").html(data); loadData(); })
                .fail(function(xhr, status, error){  $("#result").html("ERROR: " + error); loadData(); });
        });
        $("#replicate-pause-btn").click(function(){
            $.post("/api/mgmt/toggle-replicate-pause", {})
                .done(function(data){  $("#result").html(data); loadData(); })
                .fail(function(xhr, status, error){  $("#result").html("ERROR: " + error); loadData(); });
        });

        $("#remove-orphaned-blocks-btn").click(function(){
            $.post("/api/mgmt/delete-orphaned-blocks", {
                "blockListOrSingleValue" : $("#remove-orphaned-blocks-txt").val()
            })
                .done(function(data){  $("#result").html(data); loadData(); })
                .fail(function(xhr, status, error){  $("#result").html("ERROR: " + error); loadData(); });
        });

        $("#remove-queue-ops-btn").click(function(){
            $.post("/api/mgmt/delete-queue-ops", {
                "opsListOrSingleValue" : $("#remove-queue-ops-txt").val()
            })
                .done(function(data){  $("#result").html(data); loadData(); })
                .fail(function(xhr, status, error){  $("#result").html("ERROR: " + error); loadData(); });
        });


        $("#block-bootstrap-btn").click(function(){
            $.post("/api/mgmt/bootstrap", {})
                .done(function(data){  $("#result").html(data); loadData(); })
                .fail(function(xhr, status, error){  $("#result").html("ERROR: " + error); loadData(); });
        });

        $("#admin-login-btn").click(function() {
            var obj = {
                "pwd":$("#admin-login-key").val(),
                "name":$("#admin-login-name").val()
            };
            $.post("/api/auth/admin-login", obj)
                .done(function(data){  $("#result").html(data); loadData(); })
                .fail(function(xhr, status, error){  $("#result").html("ERROR: " + error);  });
        });
        // view registrations
        BLOCKS_VIEW.onReady();
        OPERATION_VIEW.onReady();
        OBJECTS_VIEW.onReady();
        API_VIEW.onReady();
        METRIC_VIEW.onReady();
        IPFS_VIEW.onReady();
        SETTINGS_VIEW.onReady();

        /////////////////////////// TAB REGISTRATION ////////////////////
        // TODO create method or file
        $("#signup-btn").click(function() {
            var obj = {
                "pwd":$("#signup-pwd").val(),
                "pwdOld":$("#signup-pwd-old").val(),
                "name":$("#signup-name").val(),
                "oauthProvider":$("#signup-oauth-p").val(),
                "oauthProviderOld":$("#signup-oauth-p-old").val(),
                "oauthId":$("#signup-oauth-id").val(),
                "oauthIdOld":$("#signup-oauth-id-old").val(),
                "algo":"EC",
                "privateKey":$("#signup-user-prk").val(),
                "privateKeyOld":$("#signup-user-prk-old").val(),
                "publicKey":$("#signup-user-pbk").val(),
                "publicKeyOld":$("#signup-user-pbk-old").val(),
                "userDetails":$("#signup-details").val()
            };
            $.post("/api/auth/signup", obj)
                .done(function(data){  $("#result").html(data); loadData(); })
                .fail(function(xhr, status, error){  $("#result").html("ERROR: " + error); loadData(); });
        });

        $("#sign-btn").click(function() {
            var obj = {
                "json":$("#sign-json").val(),
                "name":$("#sign-name").val(),
                "pwd":$("#sign-pwd").val(),
                "privateKey":$("#sign-pk").val(),
                "dontSignByServer":$("#sign-by-server").is(':checked')
            };
            $.post("/api/auth/process-operation", obj)
                .done(function(data){  $("#result").html(data); loadData();})
                .fail(function(xhr, status, error){  $("#result").html("ERROR: " + error); loadData();  });
        });

        $("#sign-add-btn").click(function() {
            var obj = {
                "name":$("#sign-name").val(),
                "pwd":$("#sign-pwd").val(),
                "privateKey":$("#sign-pk").val(),
                "addToQueue" : "true",
                "dontSignByServer":$("#sign-by-server").is(':checked')
            };
            var params = $.param(obj);
            var json = $("#sign-json").val();
            $.ajax({
                url: '/api/auth/process-operation?' + params,
                type: 'POST',
                data: json,
                contentType: 'application/json; charset=utf-8'
            }).done(function(data){  $("#result").html(data); loadData();})
                .fail(function(xhr, status, error){  $("#result").html("ERROR: " + error); loadData();  });
        });

        $("#login-btn").click(function() {
            var obj = {
                "name":$("#login-name").val(),
                "pwd":$("#login-pwd").val(),
                "edit":"true",
                "signupPrivateKey":$("#login-signup-key").val(),
                "oauthProvider":$("#login-oauth-p").val(),
                "oauthId":$("#login-oauth-id").val(),
                "loginPubKey":$("#login-public-key").val(),
                "loginAlgo":"EC",
                "userDetails": $("#login-details").val()
            };
            $.post("/api/auth/login", obj)
                .done(function(data){  $("#result").html(data); loadData();})
                .fail(function(xhr, status, error){  $("#result").html("ERROR: " + error); loadData();  });
        });

        /////////////////////////// MAIN /////////////////////////////
        window.onpopstate = function (event) {
            if (!document.location.href.toString().includes("#popover")) {
                window.location.href = document.location;
            }
        };

    });
