var BLOCKS_VIEW = function () {
    return {
        loadBlockView: function () {
            var type = $("#blocks-search").val();
            var reqObj = {
                depth: $("#block-limit-value").val()
            };
            if (type !== "all") {
                reqObj[type] = $("#search-block-field").val();
            }

            $.getJSON("/api/blocks", reqObj, function (data) {
                BLOCKS_VIEW.processBlocksResult(data);
            });
        },
        processBlocksResult: function(data) {
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
                it.find("[did='op-count']").html(op.operations_size);
                it.find("[did='block-date']").html(op.date.replace("T", " ").replace("+0000", " UTC"));

                if (op.eval) {
                    if (op.eval.superblock_hash != superblockHash) {
                        superblockHash = op.eval.superblock_hash;
                        superblockId = superblockId + 1;
                    }
                    it.find("[did='superblock']").html(superblockId + ". " + op.eval.superblock_hash);
                }
                it.find("[did='block-size']").html((op.block_size/1024).toFixed(3) + " KB");
                it.find("[did='block-objects']").html(op.obj_added + "/" + op.obj_edited + "/" + op.obj_deleted);
                it.find("[did='block-objects-info']").html("<b>" +
                    op.operations_size + "</b> operations ( <b>" +
                    op.obj_added + "</b> added, <b>" + op.obj_edited + "</b> edited, <b>" + op.obj_deleted + "</b> removed objects )");

                it.find("[did='prev-block-hash']").html(op.previous_block_hash);
                it.find("[did='merkle-tree']").html(op.merkle_tree_hash);
                it.find("[did='block-details']").html(op.details);
                it.find("[did='block-json']").html(JSON.stringify(op, null, 4));
                items.append(it);
            }
            return items;
        },
        onReady: function () {
            $("#blocks-search").change(function () {
                var type = $("#blocks-search").val();
                if (type === "all") {
                    $("#block-from-fields").addClass("hidden");
                } else {
                    $("#block-from-fields").removeClass("hidden");
                }
            });

            $("#block-load-btn").click(function () {
                var type = $("#blocks-search").val();

                BLOCKS_VIEW.loadBlockView();
                window.history.pushState(null, "State Blocks", "/api/admin?view=blocks&search=" + type+ "&hash=" + $("#search-block-field").val() + "&limit=" + $("#block-limit-value").val());
            });
        }
    }
}();

var OPERATION_VIEW = function () {
    return {
        loadOperationView: function () {
            var typeSearch = $("#operations-search").val();
            var key = $("#operations-key").val();
            if (typeSearch === "queue") {
                $.getJSON("/api/queue", function (data) {
                    generateOperationResponse(data);
                    $("#amount-operations").addClass("hidden");
                });
            } else if (typeSearch === "id") {
                $.getJSON("/api/ops-by-id?id=" + key, function (data) {
                    generateOperationResponse(data);
                    $("#amount-operations").removeClass("hidden");

                })
            } else if (typeSearch === "blockId") {
                $.getJSON("/api/ops-by-block-id?blockId=" + key, function (data) {
                    generateOperationResponse(data);
                    $("#amount-operations").removeClass("hidden");

                });
            } else { //blockHash
                $.getJSON("/api/ops-by-block-hash?hash=" + key, function (data) {
                    generateOperationResponse(data);
                    $("#amount-operations").removeClass("hidden");

                });
            }
        },
        onReady: function() {
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

            $("#operations-search-btn").click(function () {
                var typeSearch = $("#operations-search").val();
                var key = $("#operations-key").val();

                OPERATION_VIEW.loadOperationView();
                window.history.pushState(null, "State Operations", '/api/admin?view=operations&loadBy=' + typeSearch + '&key=' + key);
            });
        }
    };

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

                it.find("[did='op-hash']").html(smallHash(op.hash)).attr("href", "/api/admin?view=objects&filter=operation&key=" + op.hash + "&history=true&limit=50");
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
    }
}();

var OBJECTS_VIEW = function () {
    return {
        loadObjectsData: function() {
            $("#index-list-id").hide();
            let selectType = $("#type-list").val();
            $.ajax({
                url: "/api/objects?type=sys.operation",
                type: "GET",
                async: false,
                success: function (data) {
                    var types = "<option value = '' disabled>Select type</option>";
                    types += "<option value = 'none' selected>none</option>";
                    $("#objects-tab").html("Objects (" + data.objects.length + ")");
                    for (var i = 0; i < data.objects.length; i++) {
                        let obj = data.objects[i];
                        let objType = obj.id[0];
                        globalObjectTypes[objType] = obj;

                        types += "<option value = '" + objType + "' " +
                            (selectType === objType ? "selected" : "") + ">" + objType + "</option>";
                    }
                    $("#type-list").html(types);
                    $("#index-op-types").html(types);
                }
            });
        },
        loadObjectView: function() {
            var filter = $("#filter-list").val();
            var searchType = $("#search-type-list").val();
            var type = $("#type-list").val();
            var key = $("#search-key").val();
            $("#json-editor-div").addClass("hidden");
            $("#stop-edit-btn").addClass("hidden");
            $("#finish-edit-btn").addClass("hidden");
            $("#add-edit-op-btn").addClass("hidden");

            if (filter === "operation") {
                let amount = getHistoryForObject("operation", key, true);
                $("#amount-objects").html("Count results: " + amount);
            } else if (filter === "userid") {
                let amount = getHistoryForObject("user", key, true);
                $("#amount-objects").html("Count results: " + amount);
            } else {
                if (searchType === "count") {
                    $.getJSON("/api/objects-count?type=" + type, function (data) {
                        var items = "";
                        globalObjects = [];
                        $("#objects-list").html(items);
                        $("#amount-objects").html("Count results: " + data.count);
                    });
                } else {
                    let funcProcResults = function (data) {
                        var items = $("#objects-list");
                        items.empty();
                        let templateItem = $("#objects-list-item");
                        var amountResults = 0;
                        globalObjects = data.objects;
                        for (var i = 0; i < data.objects.length; i++) {
                            let obj = data.objects[i];
                            const tmp_id = i;
                            if ($("#historyCheckbox").is(":checked") === true) {
                                amountResults += getHistoryForObject("object", type + "," + obj.id, false);
                            } else {
                                var it = templateItem.clone();
                                it.find("[did='edit-object']").click(function () {
                                    generateJsonFromObject(globalObjects[tmp_id]);
                                });
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
                    };
                    if (searchType === "all") {
                        if (type === "none" && $("#historyCheckbox").is(":checked") === true) {
                            var amount = getHistoryForObject("all", null, true);
                            $("#amount-objects").html("Count results: " + amount);
                        } else {
                            let obj = {
                                "type": type,
                                "limit": $("#limit-field").val()
                            };

                            $.getJSON("/api/objects", obj, function (data) {
                                funcProcResults(data);
                            });
                        }

                    } else if (searchType === "id") {
                        let obj = {
                            "type": type,
                            "key": key
                        };
                        $.getJSON("/api/objects-by-id", obj, function (data) {
                            funcProcResults(data);
                        });
                    } else {
                        let obj = {
                            "type": type,
                            "index": $("#search-type-list").val(),
                            "limit": $("#limit-field").val(),
                            "key": key
                        };
                        $.getJSON("/api/objects-by-index", obj, function (data) {
                            funcProcResults(data);
                        });
                    }
                }
            }
        },
        onReady: function() {
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

            $("#load-objects-btn").click(function () {
                var filter = $("#filter-list").val();
                var searchType = $("#search-type-list").val();
                var type = $("#type-list").val();
                var key = $("#search-key").val();
                $("#json-editor-div").addClass("hidden");
                $("#stop-edit-btn").addClass("hidden");
                $("#finish-edit-btn").addClass("hidden");
                $("#add-edit-op-btn").addClass("hidden");

                OBJECTS_VIEW.loadObjectView();
                window.history.pushState(null, "State Objects", '/api/admin?view=objects&filter=' + filter +'&search=' + type + '&type=' + searchType + '&key=' + key + "&history=" + $("#historyCheckbox").is(":checked") + '&limit=' + $("#limit-field").val());
            });

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
                    $("#stop-edit-btn").removeClass("hidden");
                    $("#finish-edit-btn").addClass("hidden");
                    $("#add-edit-op-btn").removeClass("hidden");
                } catch (e) {
                    alert(e);
                }
            });

            $("#stop-edit-btn").click(function () {
                editor = null;
                originObject = null;
                $("#json-editor-div").addClass("hidden");
                $("#stop-edit-btn").addClass("hidden");
                $("#finish-edit-btn").addClass("hidden");
                $("#add-edit-op-btn").addClass("hidden");
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
                })
                    .done(function(data) {
                        $("#result").html(data); loadData();  editor = null;
                        $("#json-editor-div").addClass("hidden");
                        $("#stop-edit-btn").addClass("hidden");
                        $("#add-edit-op-btn").addClass("hidden");
                    })
                    .fail(function(xhr, status, error){  $("#result").html("ERROR: " + error); });
            });

            $("#type-list").change(function () {
                var type = $("#type-list").val();
                loadSearchTypeByOpType(type);
            });
        }
    };
    function generateJsonFromObject(obj) {
        $("#json-editor-div").removeClass("hidden");

        var target = $("#json-display");
        if (target.length) {
            event.preventDefault();
            $('html, body').stop().animate({
                scrollTop: target.offset().top
            }, 1000);
        }
        $("#finish-edit-btn").removeClass("hidden");
        $("#stop-edit-btn").removeClass("hidden");
        console.log(obj);
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
        $.ajax({
            url: "/api/history", type: "get", async: false, data: obj,
            success: function (data) {
                var templateItem = $("#objects-history-list-item");
                for (var i = 0; i < data.length; i++) {
                    var object = data[i];
                    items.append(getHistoryObjects(object, templateItem));
                }
                amount += data.length;
            },
            error: function (xhr, status, error) {
                $("#result").html("ERROR: " + error);
            }
        });
        return amount;
    }
    function loadSearchTypeByOpType(type) {
        $.ajax({
            url: "/api/indices-by-type?type=" + type, type: "get", async: false,
            success: function (data) {
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
            error: function (xhr, status, error) {
                $("#result").html("ERROR: " + error);
            }
        });
    }
}();