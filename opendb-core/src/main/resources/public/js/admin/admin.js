    var loginName = "";
    var metricsData = [];
    var globalObjectTypes = {};
    var globalConfig = {};


    function smallHash(hash) {
    	var ind = hash.lastIndexOf(':');
    	if(ind >= 0) {
    		hash = hash.substring(ind + 1);
    	}
    	return hash.substring(0, 16);
    }
    
    function loadData() {
        refreshUser();
        $.getJSON( "/api/auth/admin-status", function( data ) {
            loginName = data.admin;
            refreshUser();
            loadAllData();
        });
    }

    function loadAllData() {
        loadStatusData();
        loadObjectsData();
        $.getJSON("/api/blocks", function( data ) {
           processBlocksResult(data);
        });
        if (loginName != "") {
            loadMetricsData();
            loadBotData();
            loadDBIndexData();
            loadReportFiles();
            loadConfiguration();
            loadErrorsData();
            loadIpfsStatusData();
        }
        if (window.location.search !== "") {
            checkUrlParam();
        }
    }

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
    
    function setMetricsDataToTable() {
    	var gid = 0;
    	if($("#metrics-a").prop("checked")) {
    		gid = 1;
    	}
    	if($("#metrics-b").prop("checked")) {
    		gid = 2;
    	}
    	var items = "";
        items += "<table border='1'>";
        items += "<tr>";
        items += "<th><b>Id</b><th>";
        items += "<th><b>Count</b><th>";
        items += "<th><b>Total (s)</b><th>";
        items += "<th><b>Average (ms)</b><th>";
        items += "<th><b>Throughput / sec</b><th>";
        items += "</tr>";
        
        for(var i = 0; i < metricsData.length; i++)  {
            let item = metricsData[i];
            items += "<tr>";
            var lid = item.id;
            if(lid.length > 50) {
                lid = lid.substring(0, 50);
            }
            items += "<td><b>"+lid+"</b><td>";
            items += "<td>"+item.count[gid]+"<td>";
            items += "<td>"+item.totalSec[gid]+"<td>";
            items += "<td>"+item.avgMs[gid]+"<td>";
            if(item.avgMs > 0) {
                items += "<td>"+Number(1000/item.avgMs[gid]).toFixed(2)+"<td>";
            } else {
                items += "<td>-<td>";
            } 
            items += "</tr>";
        }
        items += "</table>";
        $("#metrics-list").html(items);
    }
    
    function loadMetricsData() {
        $.getJSON( "/api/metrics", function( data ) {
        	metricsData = data.metrics;
        	setMetricsDataToTable();
        });
    }
    
    function loadSearchTypeByOpType(type) {
        $.ajax({url:"/api/indices-by-type?type=" + type, type:"get", async:false,
            success: function(data) {
                var searchTypes = "";
                searchTypes += "<option value = 'all' selected>all</option>";
                searchTypes += "<option value = 'id' >by id</option>";
                searchTypes += "<option value = 'count' >count</option>";
                for (var i = 0; i < data.length; i++) {
                    var obj = data[i];
                    searchTypes += "<option value = " + obj.indexId + ">by " + obj.indexId + "</option>";
                }

                $("#search-type-list").html(searchTypes);
                $("#historyCheckbox").prop("disabled", false);
                $("#search-key-input").addClass("hidden")
            },
            error: function(xhr, status, error) { $("#result").html("ERROR: " + error); }
        });

    }

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

    function loadObjectsData() {
        $("#index-list-id").hide();
        let selectType = $("#type-list").val();
        $.ajax({
            url: "/api/objects?type=sys.operation",
            type: "GET",
            async: false,
            success: function(data) {
                var types = "<option value = '' selected disabled>Select type</option>";
                types += "<option value = 'none' >none</option>";
                $("#objects-tab").html("Objects (" + data.objects.length + ")");
                for(var i = 0; i < data.objects.length; i++)  {
                    let obj = data.objects[i];
                    let objType = obj.id[0];
                    globalObjectTypes[objType] = obj;

                    types += "<option value = '"+objType+"' " +
                        (selectType === objType ? "selected" : "") + ">"+objType+"</option>";
                }
                $("#type-list").html(types);
                $("#index-op-types").html(types);
            }
        });
    }

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

    function loadIpfsStatusData() {
        $.getJSON("/api/ipfs/status?full=false", function ( data ) {
            $("#ipfs-status").html(data.status);
            $("#ipfs-peer-id").html(data.peerId);
            $("#ipfs-version").html(data.version);
            $("#ipfs-gateway").html(data.gateway);
            $("#ipfs-api").html(data.api);
            $("#ipfs-addresses").html(JSON.stringify(data.addresses));
            $("#ipfs-public-key").html(data.publicKey);

            // IPFS STORAGE
            $("#ipfs-repo-size").html(data.repoSize);
            $("#ipfs-max-storage-size").html(data.storageMax);
            $("#ipfs-repo-path").html(data.repoPath);

            // IPFS SYSTEM INFO
            $("#system-info").html(data.diskInfo);
            $("#system-memory").html(data.memory);
            $("#system-runtime").html(data.runtime);
            $("#system-network").html(data.network);

            // IPFS STATS
            $("#amount-all-ipfs-objects").html(data.amountIpfsResources);
            $("#amount-pinned-ipfs-objects").html(data.amountPinnedIpfsResources);
        });
    }
    

    function processBlocksResult(data) {
        var items = $("#blocks-list");
        items.empty();
        var blocks = data.blocks;
        // SHOW currentBlock, currentTx - as in progress or failed
        $("#blocks-tab").html("Blocks (" + data.blockDepth + ")");
        var superblockId = 0;
        var superblockHash = "";
        let templateItem = $("#blocks-list-item");
        for(var i = 0; i < blocks.length; i++) {
            let op = blocks[i];
            var it = templateItem.clone();
            it.find("[did='block-operation-link']").attr("href", "/api/admin?view=operations&loadBy=blockId&key=" + op.block_id);
            it.find("[did='block-id']").html(op.block_id);
            it.find("[did='signed-by']").html(op.signed_by);
            it.find("[did='block-hash']").attr('data-content', op.hash).html(smallHash(op.hash)).popover();
            it.find("[did='op-count']").html(op.eval.operations_size);
            it.find("[did='block-date']").html(op.date.replace("T", " ").replace("+0000", " UTC"));

            if (op.eval) {
                if (op.eval.superblock_hash != superblockHash) {
                    superblockHash = op.eval.superblock_hash;
                    superblockId = superblockId + 1;
                }

                it.find("[did='superblock']").html(superblockId + ". " + op.eval.superblock_hash);
                it.find("[did='block-size']").html((op.eval.block_size/1024).toFixed(3) + " KB");
                it.find("[did='block-objects']").html(op.eval.obj_added + "/" + op.eval.obj_edited + "/" + op.eval.obj_deleted);
                it.find("[did='block-objects-info']").html("<b>" +
                		op.eval.operations_size + "</b> operations ( <b>" +
                		op.eval.obj_added + "</b> added, <b>" + op.eval.obj_edited + "</b> edited, <b>" + op.eval.obj_deleted + "</b> removed objects )");
            }

            it.find("[did='prev-block-hash']").html(op.previous_block_hash);
            it.find("[did='merkle-tree']").html(op.merkle_tree_hash);
            it.find("[did='block-details']").html(op.details);
            it.find("[did='block-json']").html(JSON.stringify(op, null, 4));
            items.append(it); 
        }
        return items;
    }

    function loadFullIpfsStatus() {
            $.getJSON("/api/ipfs/status?full=true", function (data) {
                $("#result").html("SUCCESS: " + data);
                $("#amount-missing-ipfs-objects").html(data.missingResources.length);
                $("#amount-db-objects").html(data.amountDBResources);
                $("#amount-unactivated-objects").html(data.deprecatedResources.length);

                console.log("tsts");
                for(var i = 0; i < data.missingResources.length; i++)  {
                    $("#ipfs-missing-objects").html(data.missingResources[i].hash + " , ");
                };
                for(var i = 0; i < data.deprecatedResources.length; i++)  {
                    $("#blockchain-unactivated-images").append(data.deprecatedResources[i].hash + " , ");
                };
            });
    }

    function editPreferenceObject(i) {
        console.log(globalConfig[i]);
        if (globalConfig[i].type === "Map") {
            $("#settings-edit-modal .modal-body #edit-preference-value").val(JSON.stringify(globalConfig[i].value, null, 4));
        } else {
            $("#settings-edit-modal .modal-body #edit-preference-value").val(globalConfig[i].value);

        }
        $("#settings-edit-modal .modal-body #settings-type-edit-header").html("Preference type: " + globalConfig[i].type);
        $("#settings-edit-modal .modal-body #settings-type").html(globalConfig[i].type);
        $("#settings-edit-modal .modal-body #edit-preference-restart").val(globalConfig[i].restartIsNeeded.toString());
        $("#settings-edit-modal .modal-header #settings-name").val(globalConfig[i].id);
        $("#settings-edit-modal .modal-header #settings-edit-header").html("Preference: " + globalConfig[i].id);
    }

    function loadConfiguration() {
        $.getJSON("/api/mgmt/config", function (data) {
            // var items = "";
            var items = "";
            globalConfig = data;

            function showSettings() {
                items += "<div class=\"panel panel-default col-7\">";
                items += "<div class=\"panel-heading\">";
                if (obj.canEdit === true) {
                    items += "<button type=\"button\" class=\"btn btn-default btn-sm pull-right\" data-toggle=\"modal\" data-target=\"#settings-edit-modal\" onclick=\"editPreferenceObject(" + i + ")\">Edit</button></a>";
                }
                items += "<h4>" + obj.id + " - " + obj.description + "</h4></div><div class=\"panel-body\">";

                if (obj.type === "Map") {
                    items += "<h5><b>Value: </b></h5>";
                    items += "<pre>" + JSON.stringify(obj.value, null, 4) + "</pre>"
                } else {
                    items += "<h5><b>Value: </b>" + obj.value + "</h5>";
                }
                items += "<div class=\"row\">";
                if (obj.restartIsNeeded === true) {
                    items += "<div class=\"alert alert-warning\" role=\"alert\">This parameter will be accepted after server restarting!</div>";
                }
                items += "</div>";
                items += "</div>";
                items += "</div>";
            }

            for (var i = 0; i < data.length; i++) {
                obj = data[i];
                if ($("#settingsCheckbox").is(":checked") && obj.canEdit === true) {
                    showSettings();
                } else if (!($("#settingsCheckbox").is(":checked"))) {
                    showSettings();
                }

            }

            $("#settings-result-list").html(items);
        });
    }

    function loadBotData() {
        $.getJSON("/api/bot/stats", function (data) {
            var items = "";
            for (var key in data) {
                var obj = data[key];
                items += "<tr>";
                items += "<td>" + obj.id + "</td>";
                if ('botStats' in obj) {
                items += "<td>" + obj.botStats.taskName + "</td>";
                items += "<td>" + obj.botStats.taskDescription + "</td>";
                } else {
                    items += "<td></td>";
                    items += "<td></td>";
                }
                items += "<td>" + new Date(obj.started).toLocaleString() + "</td>";
                items += "<td>" + obj.interval + "</td>";
                if ('botStats' in obj) {
                    if (!obj.botStats.isRunning) {
                        items += "<td>NOT RUNNING</td>"
                    } else {
                        var progressBarValue = parseInt((obj.botStats.progress / obj.botStats.total) * 100);
                        items += "<td><div class=\"progress\">\n" +
                            "  <div class=\"progress-bar progress-bar-info progress-bar-striped\" role=\"progressbar\"\n" +
                            "  aria-valuenow=\"" + progressBarValue + "\" aria-valuemin=\"0\" aria-valuemax=\"100\" style=\"width:" + progressBarValue + "%\">\n" +
                            progressBarValue + "%" +
                            "  </div>\n" +
                            "</div></td>";
                    }
                } else {
                    items += "<td></td>";
                }
                items += "<td>";
                if ('botStats' in obj && obj.botStats.isRunning === false) {
                    items += "<button type=\"button\" class=\"btn btn-primary\" style=\"margin-left:5px;\" onclick=\"startBot('" + obj.id + "')\"><span class=\"glyphicon glyphicon-play\"></span></button>";
                } else {
                    items += "<button type=\"button\" class=\"btn btn-primary\" style=\"margin-left:5px;\" onclick=\"stopBot('" + obj.id + "')\"><span class=\"glyphicon glyphicon-pause\"></span></button>";
                }
                items += "<button type=\"button\" class=\"btn btn-primary\" data-toggle=\"modal\" data-target=\"#bot-history-modal\" style=\"margin-left:5px;\" onclick=\"showBotHistory('" + obj.id + "')\"><span class=\"glyphicon glyphicon-eye-open\"></span></button>";
                items += "</td>";
                items += "</tr>";

            }

            $("#bots-table > tbody").html(items);
        });
    }

    function loadReportFiles() {
    	// TODO
    }

    function loadDBIndexData() {
        $.getJSON("/api/index", function (data) {
            var items = "";
            for (var key in data) {
                var obj = data[key];
                items += "<tr><td class='info' colspan='9'>" + key + "</td></tr>";
                for (var keyObj in obj) {
                    var index = obj[keyObj];
                    items += "<tr>";
                    items += "<td>" + index.indexId + "</td>";
                    items += "<td>" + index.opType + "</td>";
                    items += "<td>" + index.columnDef.tableName + "</td>";
                    items += "<td>" + index.columnDef.colName + "</td>";
                    items += "<td>" + index.columnDef.colType + "</td>";
                    items += "<td>" + index.columnDef.index + "</td>";
                    if (index.fieldsExpression.length >= 1) {
                        items += "<td>" + index.fieldsExpression[0].expression + "</td>";
                    } else {
                        items += "<td></td>";
                    }
                    items += "<td>" + index.cacheRuntimeBlocks + "</td>";
                    items += "<td>" + index.cacheRuntimeBlocks + "</td>";
                    items += "</tr>";
                }
            }
            $("#index-table > tbody").html(items);
        })
    }

    function generateOperationResponse(data) {
        var items = $("#operations-list");
        items.empty();
        let templateItem = $("#operation-list-item");
        if (JSON.stringify(data) !== '{}') {
            for (var i = 0; i < data.ops.length; i++) {
                var it = templateItem.clone();
                let op = data.ops[i];
                let clone = JSON.parse(JSON.stringify(op));
                delete clone.hash;
                delete clone.signature;
                delete clone.signature_hash;
                delete clone.validation;
                delete clone.eval;
                if (clone.create) {
                    for (var ik = 0; ik < clone.create.length; ik++) {
                        if (clone.create[ik].changesForObject) {
                            for (var il = 0; il < clone.create[ik].changesForObject.length; il++) {
                                delete clone.create[ik].changesForObject[il].eval;
                            }
                        }
                        delete clone.create[ik].eval;
                    }
                }
                if (clone.edit) {
                    for (var j = 0; j < clone.edit.length; j++) {
                        delete clone.edit[j].eval;
                    }
                }

                if (clone.delete) {
                    for (var j = 0; j < clone.delete.length; j++) {
                        delete clone.delete[j].eval;
                    }
                }

                sha256(JSON.stringify(clone)).then(digestValue => {
                    if(("json:sha256:" + digestValue) != op.hash){
                    alert("Warning! Hash of tx '" + op.hash + "' is not correct - 'json:sha256:" + digestValue + "'");
                }});


                var create = "";
                if (Array.isArray(op.create)) {
                    if (op.create.length > 1) {
                        create = op.create.length;
                    } else {
                        create = op.create.length + " [" + op.create[0].id + "]";
                    }
                } else {
                    if (op.create) {
                        create = "1 [" + op.create.id + "]";
                    }
                }
                var edited = "";
                if (Array.isArray(op.edit)) {
                    if (op.edit.length > 1) {
                        edited = op.edit.length;
                    } else {
                        edited = op.edit.length + " [" + op.edit[0].id + "]";
                    }
                } else {
                    if (op.edit) {
                        edited = "1 [" + op.edit.id + "]";
                    }
                }
                var deleted = "";
                if (Array.isArray(op.delete)) {
                    if (op.delete.length > 1) {
                        deleted = op.delete.length;
                    } else {
                        deleted = op.delete.length + " [" + op.delete[0] + "]";
                    }
                } else {
                    if (op.delete) {
                        deleted = "1 [" + op.delete + "]";
                    }
                }

                var objectInfo = "";
                if (create !== "") {
                    objectInfo +=   "<b>" + create + "</b> added "
                }
                if (edited !== "") {
                    objectInfo +="<b>" + edited + "</b> edited "
                }
                if (deleted !== "") {
                    objectInfo +=  "<b>" + deleted + "</b> removed ";
                }

                it.find("[did='op-hash']").html(smallHash(op.hash)).attr("href", "/api/admin?view=objects&filter=operation&search=null&type=opHash&key=" + op.hash + "&history=true&limit=50");
                it.find("[did='op-type']").html(op.type);
                it.find("[did='op-type-link']").attr("href", "/api/admin?view=objects&filter=type&search=" + op.type + "&type=all&history=false&limit=50");
                it.find("[did='object-info']").html(objectInfo);
                it.find("[did='signed-by']").html(op.signed_by);
                it.find("[did='op-json']").html(JSON.stringify(op, null, 4));
                items.append(it);
            }
            $("#amount-operations").html("Amount operations: " + data.ops.length);
        } else {
            $("#amount-operations").html("Amount operations: " + 0);
        }
        // $("#operations-list").html(items);
    }

    var editor = {};
    var originObject;
    var globalObjects = {};
    function generateJsonFromObject(obj) {
        $("#json-display").show();

        var target = $("#json-display");
        if( target.length ) {
            event.preventDefault();
            $('html, body').stop().animate({
                scrollTop: target.offset().top
            }, 1000);
        }
        $("#editing-objects").show();
        $("#stop-edit-objects").show();
        delete obj.eval;
        originObject = obj;
        editor = new JsonEditor('#json-display', obj);
        editor.load(obj);
    }

    function getHistoryObjects(obj, templateItem) {
        var it = templateItem.clone();

        it.find("[did='object-op-hash']").attr('data-content', obj.opHash).html(smallHash(obj.opHash)).popover();
        it.find("[did='obj-id']").html(obj.id.toString());
        it.find("[did='obj-date']").html(obj.date);
        if (obj.objEdit.eval !== undefined) {
            it.find("[did='obj-type']").html(obj.objType);
        }
        if (obj.objEdit.comment !== undefined) {
            it.find("[did='obj-comment']").html(obj.objEdit.comment);
        } else {
            it.find("[did='comment-hidden']").addClass("hidden");
        }
        if (obj.userId.length !== 0) {
            it.find("[did='obj-user']").html(obj.userId);
        } else {
            it.find("[did='user-hidden']").addClass("hidden")
        }
        it.find("[did='obj-status']").html(obj.status);
        if ('objEdit' in obj) {
            delete obj.objEdit.eval;
            it.find("[did='object-json']").html(JSON.stringify(obj.objEdit, null, 4));
        } else {
            it.find("[did='object-json-hidden']").addClass("hidden");
        }
        if ('deltaChanges' in obj) {
            it.find("[did='delta-json']").html(JSON.stringify(obj.deltaChanges, null, 4));
        } else {
            it.find("[did='delta-json-hidden']").addClass("hidden");
        }

        return it;
    }

    function getHistoryForObject(searchType, key, clear) {
        var obj = {
            "type": searchType,
            "key": key,
            "limit": $("#limit-field").val(),
            "sort": "DESC"
        };
        var items = $("#objects-list");
        if (clear) {
            items.empty();
        }
        var amount = 0;
        $.ajax({url:"/api/history", type:"get", async:false, data:obj,
            success: function(data) {
                var templateItem = $("#objects-history-list-item");
                for (var i = 0; i < data.length; i++) {
                    var object = data[i];
                    items.append(getHistoryObjects(object, templateItem));
                }
                amount += data.length;
            },
            error: function(xhr, status, error) { $("#result").html("ERROR: " + error); }});
        return amount;
    }

    function saveCurrentState(tab, url) {
        // if (window.location.pathname + window.location.search !== url && url !== window.location.pathname && url !== (window.location.pathname + "?")) {
        //     if (tab === "#objects") {
        //         saveObjectTabState(tab, url);
        //     } else if (tab === "#operations") {
        //         saveOperationTabState(tab, url);
        //     } else if (tab === "#blocks") {
        //         saveBlocksTabState(tab, url);
        //     }
        // }
    }

    function saveBlocksTabState(tab, url) {
        var idStorage = '_' +  Math.random().toString(36).substr(2, 9);
        var res = {
            tab: tab,
            blockSearch: $("#blocks-search").val(),
            showInput: $("#block-from-fields").hasClass("hidden"),
            inputKey: $("#search-block-field").val(),
            inputLimit: $("#block-limit-value").val(),
            idStorage: idStorage
        };

        sessionStorage.setItem(idStorage,  document.getElementById("blocks-list").innerHTML);
        window.history.pushState(res, "State Blocks", url);
    }

    function saveOperationTabState(tab, url) {
        var idStorage = '_' +  Math.random().toString(36).substr(2, 9);
        var res = {
            tab: tab,
            inputSearch:$("#input-search-operation-key").text(),
            keyHidden:$("#operations-search-fields").is(":hidden"),
            keyValue: $("#operations-key").val(),
            load: $("#operations-search").val(),
            countElements: $("#amount-operations").text(),
            idStorage: idStorage
        };

        sessionStorage.setItem(idStorage, document.getElementById("operations-list").innerHTML);
        window.history.pushState(res, "State Operations", url);
    }

    function saveObjectTabState(tab, url) {
        var idStorage = '_' + Math.random().toString(36).substr(2, 9);
        var res = {
            tab: tab,
            searchType: $("#search-type-list").val(),
            type: $("#type-list").val(),
            showHistory: $("#historyCheckbox").is(":checked"),
            historyDisabled: $("#historyCheckbox").is(":disabled"),
            key: $("#search-key").val(),
            limit: $("#limit-field").val(),
            countElements: $("#amount-objects").text(),
            idStorage: idStorage
        };

        sessionStorage.setItem(idStorage, document.getElementById("objects-list").innerHTML);
        console.log(res);
        window.history.pushState(res, "State Objects", url);
        console.log(window.history);
    }

    function showOperations(data, url) {
        generateOperationResponse(data);
    }

    function checkUrlParam() {
        // objects -> http://localhost:6463/api/admin?view=objects&filter=&search=osm.place&type=id&key=12345662&history=true
        // operations -> http://localhost:6463/api/admin?view=operations&loadBy=blockId&key=0
        // blocks -> http://localhost:6463/api/admin?view=blocks&search=from&hash=213&limit=123
        var url = new URL(window.location.href);
        if (window.location.href.toLowerCase().indexOf('api/admin?view=objects') > 1) {
            $(".nav-tabs a[href=\"#objects\"]").tab('show');
                var filter = url.searchParams.get('filter');
                if (filter !== null) {
                    $("#filter-list").val(filter);
                    $("#filter-list").change();
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

                $("#load-objects-btn").click();
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

            $("#operations-search-btn").click();
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

            $("#block-load-btn").click();
        }
    }

    function startBot(bot) {
        var obj = {
            "botName":bot
        };
        $.post("/api/bot/start", obj)
            .done(function (data) {$("#result").html(data)})
            .fail(function(xhr, status, error) { $("#result").html("ERROR: " + error); });
    }

    function stopBot(bot) {
        var obj = {
            "botName":bot
        };
        $.post("/api/bot/stop", obj)
            .done(function (data) {$("#result").html(data)})
            .fail(function(xhr, status, error) { $("#result").html("ERROR: " + error); });
    }

    function showBotHistory(bot) {
        var obj = {
            "botName" : bot
        };
        //[{"bot":"opr-type","status":"SUCCESS","startDate":"Sep 5, 2019, 11:50:45 AM","endDate":"Sep 5, 2019, 11:51:40 AM","total":25858,"processed":25858}]
        $.getJSON("/api/bot/history", obj)
            .done(function (data) {
                var items = "";
                for (var i=0; i < data.length; i++) {
                    var obj = data[i];
                    items += "<tr>";
                    items += "<td>" + obj.bot + "</td>";
                    items += "<td>" + new Date(obj.startDate).toLocaleString() + "</td>";
                    if (obj.endDate !== undefined) {
                        items += "<td>" + new Date(obj.endDate).toLocaleString() + "</td>";
                    } else {
                        items += "<td></td>"
                    }
                    items += "<td>" + obj.total + "</td>";
                    items += "<td>" + obj.processed + "</td>";
                    items += "<td>" + obj.status + "</td>";
                    items += "</tr>";
                }
                $(".modal-body #bots-history-table  > tbody").html(items);
                $(".modal-header #history-bot-name").val(bot);
                $(".modal-header #bot-history-header").html("History of the launches for bot: " + bot);
            })
            .fail(function(xhr, status, error) { $("#result").html("ERROR: " + error); });
    }


    function closeBotHistory() {
        $("#bots-history-table").hide();
    }

    $( document ).ready(function() {
        loadData();
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

        $("#config-list").change(function () {
            var selected = $("#config-list").val();
            if (globalConfig[selected].toString() === "[object Object]") {
                $("#config-value").val(JSON.stringify(globalConfig[selected], null, 4)).attr("rows", 20);
            } else {
                $("#config-value").val(globalConfig[selected]).attr("rows", 1);
            }
        });

        $("#filter-list").change(function() {
           var selected = $("#filter-list").val();
           if (selected === "type") {
               $("#type-list-select").removeClass("hidden");
               $("#search-type-list-select").removeClass("hidden");
               $("#search-key-input").addClass("hidden");
               $("#historyCheckbox").prop("disabled", false).prop('checked', false);
           } else {
               $("#type-list-select").addClass("hidden");
               $("#search-type-list-select").addClass("hidden");
               $("#search-key-input").removeClass("hidden");
               $("#historyCheckbox").prop("disabled", true).prop('checked', true);
               if (selected === "operation") {
                   $("#name-key-field").text("Input operation Hash");
               } else {
                   $("#name-key-field").text("Input user Id");
               }
           }
        });

        $("#settingsCheckbox").change(function () {
           loadConfiguration();
        });

        $("#search-type-list").change(function() {
            var selectedSearchType = $("#search-type-list").val();
            if (selectedSearchType === "all") {
                if ($("#type-list").val() === "none") {
                    $("#historyCheckbox").prop('checked', true)
                } else {
                    $("#search-key-input").addClass("hidden");
                    $("#historyCheckbox").prop("disabled", false);
                }
            } else if ( selectedSearchType === "count") {
                $("#search-key-input").addClass("hidden");
                $("#historyCheckbox").prop("disabled", true).prop('checked', false);
            } else if (selectedSearchType === "userId") {
                $("#search-key-input").removeClass("hidden");
                $("#name-key-field").text("Input User ID");
                $("#historyCheckbox").prop('checked', true).prop("disabled", true);
            } else if (selectedSearchType === "opHash") {
                $("#search-key-input").removeClass("hidden");
                $("#type-list").val("none");
                $("#name-key-field").text("Input Op Hash");
                $("#historyCheckbox").prop('checked', true).prop("disabled", true);
            } else {
                $("#search-key-input").removeClass("hidden");
                if (selectedSearchType === "id") {
                    $("#name-key-field").text("Input Object ID");
                } else {
                    $("#name-key-field").text("Input " + $("#search-type-list").val());
                }
                $("#historyCheckbox").prop("disabled", false);
            }
        });


        $("#type-list").change(function () {
            var type = $("#type-list").val();
            loadSearchTypeByOpType(type);
        });

        $("#operations-search").change(function () {
           var type = $("#operations-search").val();
           if (type === "queue") {
               $("#operations-search-fields").addClass("hidden");
           } else {
               $("#operations-search-fields").removeClass("hidden");
               if (type === "id") {
                   $("#input-search-operation-key").text("Input object id");
               } else if (type === "blockId") {
                   $("#input-search-operation-key").text("Input block Id");
               } else {
                   $("#input-search-operation-key").text("Input block hash");
               }
           }
        });

        $("#refresh-index-table-btn").click(function () {
           loadDBIndexData();
        });

        $("#add-new-index-btn").click(function () {
            var myJSObject = {
                tableName:$("#index-table-name").val(),
                colName:$("#index-col-name").val(),
                colType:$("#index-col-type").val(),
                types:$("#index-op-types").val(),
                index:$("#index-type").val(),
                sqlMapping:$("#index-col-sql-mapping").val(),
                cacheRuntimeMax:$("#index-cacheRuntimeMax").val(),
                cacheDbIndex:$("#index-cacheDbIndex").val(),
                field:($("#index-field").val()).split(",")
            };
            $.ajax("/api/mgmt/index", {
                data: JSON.stringify(myJSObject),
                contentType: 'application/json',
                type: 'POST',
                success: function(data) {

                },
                error: function(xhr, status, error) {

                }
            });
            $("#new-index-modal .close").click();
        });

        $("#refresh-api-table-btn").click(function () {
            loadReportFiles();
        });

        $("#refresh-bot-table-btn").click(function () {
            loadBotData();
        });

        $("#blocks-search").change(function () {
           var type = $("#blocks-search").val();
           if (type === "all") {
               $("#block-from-fields").addClass("hidden");
           } else {
               $("#block-from-fields").removeClass("hidden");
           }
        });
        
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

        $("#load-objects-btn").click(function () {
            var filter = $("#filter-list").val();
            var searchType = $("#search-type-list").val();
            var type = $("#type-list").val();
            var key = $("#search-key").val();
            $("#json-display").hide();
            $("#editing-objects").hide();
            $("#stop-edit-objects").hide();
            $("#add-edited-obj").hide();

            if (filter === "operation") {
                let amount = getHistoryForObject("operation", key, true);
                $("#amount-objects").html("Count results: " +  amount);
                //saveCurrentState('#objects', '/api/admin?view=objects&filter=operation&search=' + type + '&type=' + searchType + '&key=' + key + "&history=" + $("#historyCheckbox").is(":checked") + '&limit=' + $("#limit-field").val());
            } else if (filter === "userid") {
                let amount = getHistoryForObject("user", key, true);
                $("#amount-objects").html("Count results: " +  amount);
                // saveCurrentState('#objects', '/api/admin?view=objects&filter=userid&search=' + type + '&type=' + searchType + '&key=' + key + "&history=" + $("#historyCheckbox").is(":checked") + '&limit=' + $("#limit-field").val());
            } else {
                if (searchType === "count") {
                    $.getJSON("/api/objects-count?type=" + type, function (data) {
                        var items = "";
                        globalObjects = [];
                        $("#objects-list").html(items);
                        $("#amount-objects").html("Count results: " + data.count);
                        // saveCurrentState('#objects', '/api/admin?view=objects&filter=type&search=' + type + '&type=' + searchType + '&history=' + $("#historyCheckbox").is(":checked"));
                    });
                } else {
                    let funcProcResults = function (data, url) {
                        var items = $("#objects-list");
                        items.empty();
                        let templateItem = $("#objects-list-item");
                        var amountResults = 0;
                        globalObjects = data.objects;
                        for (var i = 0; i < data.objects.length; i++) {
                            let obj = data.objects[i];
                            if ($("#historyCheckbox").is(":checked") === true) {
                                amountResults += getHistoryForObject("object", type + "," + obj.id, false);
                            } else {
                                var it = templateItem.clone();
                                it.find("[did='object-op-hash']").attr('data-content', obj.eval.parentHash).html(smallHash(obj.eval.parentHash)).popover();
                                it.find("[did='obj-id']").html(obj.id.toString());
                                it.find("[did='history-object-link']").attr("href", "/api/admin?view=objects&filter=type&search=" + type + "&type=id&key=" + obj.id.toString() + "&history=true&limit=50")
                                if (obj.comment) {
                                    it.find("[did='obj-comment']").html(obj.comment);
                                } else {
                                    it.find("[did='comment']").prop("hidden", true);
                                }
                                it.find("[did='object-json']").html(JSON.stringify(obj, null, 4));

                                items.append(it)
                            }
                        }
                        $("#amount-objects").html("Count results: " + (amountResults === 0 ? data.objects.length : amountResults));
                        // saveCurrentState('#objects', url);
                    };
                    if (searchType === "all") {
                        if (type === "none" && $("#historyCheckbox").is(":checked") === true) {
                            var amount = getHistoryForObject("all", null, true);
                            $("#amount-objects").html("Count results: " +  amount);
                            // saveCurrentState('#objects', url);
                        } else {
                            let obj = {
                                "type": type,
                                "limit": $("#limit-field").val()
                            };

                            $.getJSON("/api/objects", obj, function (data) {
                                funcProcResults(data, '/api/admin?view=objects&filter=type&search=' + type + '&type=' + searchType + '&history=' + $("#historyCheckbox").is(":checked") + '&limit=' + $("#limit-field").val());
                            });
                        }

                    } else if (searchType === "id") {
                        let obj = {
                            "type": type,
                            "key": key
                        };
                        $.getJSON("/api/objects-by-id", obj, function (data) {
                            funcProcResults(data, '/api/admin?view=objects&filter=type&search=' + type + '&type=' + searchType + '&key=' + key + "&history=" + $("#historyCheckbox").is(":checked") + '&limit=' + $("#limit-field").val());
                        });
                    } else {
                        let obj = {
                            "type": type,
                            "index": $("#search-type-list").val(),
                            "limit": $("#limit-field").val(),
                            "key": key
                        };
                        $.getJSON("/api/objects-by-index", obj, function (data) {
                            funcProcResults(data, '/api/admin?view=objects&filter=type&search=' + type + '&type=' + searchType + '&key=' + key + '&history=' + $("#historyCheckbox").is(":checked") + '&limit=' + $("#limit-field").val());
                        });
                    }
                }
            }

        });

        window.onpopstate = function (event) {
            if (window.history.state && history.state.idStorage) {
                if (history.state.tab === "#objects") {
                    $(".nav-tabs a[href=\""+history.state.tab+"\"]").tab('show');
                    document.getElementById("objects-list").innerHTML = sessionStorage.getItem(event.state.idStorage);
                    console.log(history.state);
                    $("#type-list").val(history.state.type).change();
                    setTimeout(function() {
                        $("#search-type-list").val(history.state.searchType).change();
                    }, 200);
                    $("#historyCheckbox").prop("checked", history.state.showHistory).prop("disabled", history.state.historyDisabled);
                    $("#limit-field").val(history.state.limit);
                    $("#search-key").val(history.state.key);
                    $("#amount-objects").text(history.state.countElements);
                } else if (history.state.tab === "#operations") {
                    $(".nav-tabs a[href=\""+history.state.tab+"\"]").tab('show');
                    document.getElementById("operations-list").innerHTML = sessionStorage.getItem(event.state.idStorage);
                    console.log(history.state);
                    if (!history.state.keyHidden) {
                        // $("#operations-search-fields").show();
                        $("#operations-search-fields").removeClass("hidden");
                        $("#input-search-operation-key").text(history.state.inputSearch);
                        $("#operations-key").val(history.state.keyValue);
                    } else {
                        // $("#operations-search-fields").hide();
                        $("#operations-search-fields").addClass("hidden");
                        // $("#input-search-operation-key").hide();
                        // $("#operations-key").hide();
                    }
                    $("#operations-search").val(history.state.load);
                    $("#amount-operations").text(history.state.countElements);
                } else if (history.state.tab === "#blocks") {
                    $(".nav-tabs a[href=\""+history.state.tab+"\"]").tab('show');
                    document.getElementById("blocks-list").innerHTML = sessionStorage.getItem(event.state.idStorage);
                    $("#blocks-search").val(history.state.blockSearch);
                    if (!history.state.showInput) {
                    	$("#block-from-fields").removeClass("hidden");
                        $("#search-block-field").val(history.state.inputKey);
                        $("#block-limit-value").val(history.state.inputLimit);

                    } else {
                    	$("#block-from-fields").addClass("hidden");
                    }
                }
            }
        };

        $("#finish-edit-btn").click(function () {
            try {
                var newObject = editor.get();
                var res = DEEP_DIFF_MAPPER.map(originObject, newObject);
                var path = "";
                var changeEdit = {};
                var currentEdit = {};
                DEEP_DIFF_MAPPER.generateChangeCurrentObject(path, res, changeEdit, currentEdit);
                DEEP_DIFF_MAPPER.generateEditOp(originObject, changeEdit, currentEdit);

                originObject = null;
                $("#editing-objects").hide();
                $("#stop-edit-objects").show();
                $("#add-edited-obj").show();
            } catch (e) {
                alert(e);
            }
        });

        $("#stop-edit-btn").click(function () {
            editor = null;
            originObject = null;
            $("#editing-objects").hide();
            $("#add-edited-obj").hide();
            $("#stop-edit-objects").hide();
            $("#json-display").hide();
        });

       
        $("#load-bot-stats-btn").click(function () {
            $.getJSON("/api/bot/stats")
                .done(function (data) {
                    var items = "";
                    for (var key in data) {
                        var obj = data[key];
                        items += "<br><li><b>Id</b>: " + obj.id;
                        items += "<br><b>API</b>: " + obj.api;
                        items += "<br><b>Version</b>: " + obj.version;
                        items += "<br><b>Started</b>: " + obj.started;
                        if ('botStats' in obj) {
                            items += "<br><b>Task description</b>: " + obj.botStats.taskDescription;
                            items += "<br><b>Task name</b>: " + obj.botStats.taskName;
                            items += "<br><b>Task count</b>: " + obj.botStats.taskCount;
                            items += "<br><b>Total tasks</b>: " + obj.botStats.total;
                            if (obj.botStats.taskCount === obj.botStats.total) {
                                items += "<span class=\"label label-primary\">Not enabled</span>";
                            } else {
                                items += "<div class=\"progress\">\n" +
                                    "<div class=\"progress-bar progress-bar-striped progress-bar-animated\" role=\"progressbar\" aria-valuenow=\"" + obj.botStats.progress + "\" aria-valuemin=\"0\" aria-valuemax=\"" + obj.botStats.total + "\" style=\"width: " + obj.botStats.progress + "%\"></div>\n" +
                                    "</div>";
                            }
                        }
                        items += "<br></li>";
                    };
                    $("#bot-list-stats").html(items);
                })
                .fail(function(xhr, status, error) { $("#result").html("ERROR: " + error); });
        });
        
        $("#refresh-bot-history-btn").click(function () {
            var botName = $(".modal-header #history-bot-name").val();
            showBotHistory(botName);
        });

        $("#update-settings").click(function () {
            var keyConfig = $("#config-list").val();
            if (globalConfig[keyConfig] !== $("#config-value").val() && keyConfig !== null) {
                var obj = {
                    key: keyConfig,
                    value: $("#config-value").val()
                };
                $.post("/api/mgmt/config", obj)
                    .done(function (data) {
                        $("#result").html("SUCCESS: " + data);
                        loadConfiguration();
                        setTimeout(function() {
                            $("#config-list").val(keyConfig).change();
                        }, 200);
                    })
                    .fail(function (xhr, status, error) {
                        $("#result").html("ERROR: " + error);
                    });
                $("#settings-warn-alert").show();
            } else {
                $("#result").html("ERROR: " + "Nothing to update");
            }
        });

       $("#show-add-new-preference-btn").click(function () {
          $("#add-new-preference").show();
          $("#show-add-new-preference-btn").hide();
       });

       $("#add-new-preference-btn").click(function () {
           var obj = {
               key: $("#new-preference-key").val(),
               value: $("#new-preference-value").val(),
               type: $("#new-preference-type").val(),
               restartIsNeeded: $("#new-preference-restart").val()
           };
           $.post("/api/mgmt/config", obj)
               .done(function (data) {
                   $("#result").html("SUCCESS: " + data);
                   loadConfiguration();
               })
               .fail(function (xhr, status, error) {
                   $("#result").html("ERROR: " + error);
               });
       });

       $("#cancel-new-preference-btn").click(function () {
           $("#add-new-preference").hide();
           $("#show-add-new-preference-btn").show();

       });

       $("#save-new-value-for-settings-btn").click(function () {
           var obj = {
               key: $("#settings-name").val(),
               value: $("#edit-preference-value").val(),
               type: $("#settings-type").text(),
           };
           $.post("/api/mgmt/config", obj)
               .done(function (data) {
                   $("#result").html("SUCCESS: " + data);
                   loadConfiguration();
               })
               .fail(function (xhr, status, error) {
                   $("#result").html("ERROR: " + error);
               });

           $("#settings-edit-modal .close").click();
       });

        $("#operations-search-btn").click(function () {
            var typeSearch = $("#operations-search").val();
            var key = $("#operations-key").val();

            if (typeSearch === "queue") {
                $.getJSON( "/api/queue", function( data ) {
                    showOperations(data, '/api/admin?view=operations&load=' + typeSearch);
                    $("#amount-operations").addClass("hidden");
                });
            } else if (typeSearch === "id") {
                $.getJSON("/api/ops-by-id?id=" + key, function (data) {
                    showOperations(data, '/api/admin?view=operations&loadBy=' + typeSearch + '&key=' + key);
                    $("#amount-operations").removeClass("hidden");

                })
            } else if (typeSearch === "blockId") {
                $.getJSON( "/api/ops-by-block-id?blockId=" + key, function( data ) {
                    showOperations(data, '/api/admin?view=operations&loadBy=' + typeSearch + '&key=' + key);
                    $("#amount-operations").removeClass("hidden");

                });
            } else { //blockHash
                $.getJSON( "/api/ops-by-block-hash?hash=" + key, function( data ) {
                    showOperations(data, '/api/admin?view=operations&loadBy=' + typeSearch + '&key=' + key);
                    $("#amount-operations").removeClass("hidden");

                });
            }
        });

        $("#block-load-btn").click(function () {
            var type = $("#blocks-search").val();
            var reqObj = {
               	depth: $("#block-limit-value").val()
            };
            if(type != "all") {
            	reqObj[type] = $("#search-block-field").val();
            }
            $.getJSON("/api/blocks", reqObj, function (data) {
                processBlocksResult(data);
            });
        });

        $("#add-file-btn").click(function () {
            var formData = new FormData();
            formData.append("file", $("#image-file")[0].files[0]);

            $.ajax({
                url: '/api/ipfs/image',
                data: formData,
                processData: false,
                contentType: false,
                type: 'POST',
                success: function(data) {
                    $("#result-add-image").html(data.toString());
                    loadData();
                },
                error: function (xhr, status, error) {
                    $("#result-add-image").html("ERROR: " + error);
                }
            });
        });

        $("#get-image-btn").click(function () {
            $("#image-link").attr("href", "/api/ipfs/image?hash=" + $("#get-image").val());
            $("#image-link").click();
        });

        $("#fix-ipfs-missing-images-btn").click(function () {
            $.post("/api/ipfs/mgmt/ipfs-maintenance")
                .done(function (data) {$("#result").html("SUCCESS: " + data); loadData(); loadFullIpfsStatus(); })
                .fail(function (xhr, status, error) { $("#result").html("ERROR: " + error); })
        });

        $("#fix-blc-missing-images-btn").click(function () {
            $.post("/api/ipfs/mgmt/clean-deprecated-ipfs")
                .done(function (data) {
                    $("#result").html("SUCCESS: " + data);
                    loadData();
                    loadFullIpfsStatus();
                })
                .fail(function (xhr, status, error) {
                    $("#result").html("ERROR: " + error);
                });
        });

        $("#laod-full-stats-btn").click(function () {
            loadFullIpfsStatus();
        });

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

            // $.post("/api/auth/process-operation", obj)
        });
        $("#add-edit-op-btn").click(function () {
            var obj = {
                "addToQueue" : "true",
                "dontSignByServer": false
            };
            var params = $.param(obj);
            var json = JSON.stringify(editor.get());
            $.ajax({
                url: '/api/auth/process-operation?' + params,
                type: 'POST',
                data: json,
                contentType: 'application/json; charset=utf-8'
            }).done(function(data){
                $("#result").html(data); loadData();  editor = null;
                $("#json-display").hide();$("#stop-edit-objects").hide();
                $("#add-edited-obj").hide()})
                .fail(function(xhr, status, error){  $("#result").html("ERROR: " + error); });
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
    });

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

    $('input[type="checkbox"]').on('change', function() {
        $(this).siblings('input[type="checkbox"]').prop('checked', false);
    });
