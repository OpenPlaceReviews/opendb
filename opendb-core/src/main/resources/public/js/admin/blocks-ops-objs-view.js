var BLOCKS_VIEW = function () {
    function processBlocksResult (data) {
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
    }
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
                processBlocksResult(data);
            });
        },
        loadURLParams: function (url) {
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
                var url = "/api/admin?view=blocks&search=" + type;
                if ($("#search-block-field").val() !== "") {
                    url += "&hash=" + $("#search-block-field").val();
                }
                if ($("#block-limit-value").val() !== "") {
                    url += "&limit=" + $("#block-limit-value").val();
                }
                window.history.pushState(null, "State Blocks", url);
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
                getJsonAction("/api/queue", function (data) {
                    generateOperationResponse(data);
                    $("#amount-operations").addClass("hidden");
                });
            } else if (typeSearch === "id") {
                getJsonAction("/api/ops-by-id?id=" + key, function (data) {
                    generateOperationResponse(data);
                    $("#amount-operations").removeClass("hidden");

                })
            } else if (typeSearch === "blockId") {
                getJsonAction("/api/ops-by-block-id?blockId=" + key, function (data) {
                    generateOperationResponse(data);
                    $("#amount-operations").removeClass("hidden");

                });
            } else { //blockHash
                getJsonAction("/api/ops-by-block-hash?hash=" + key, function (data) {
                    generateOperationResponse(data);
                    $("#amount-operations").removeClass("hidden");

                });
            }
        },
        loadURLParams: function(url) {
            var loadType = url.searchParams.get('loadBy');
            if (loadType !== null) {
                $("#operations-search").val(loadType).change();
            }
            var key = url.searchParams.get('key');
            if (key !== null) {
                $("#operations-key").val(key);
            }
            OPERATION_VIEW.loadOperationView();
        },
        onReady: function() {
            $("#operations-search").change(function () {
                var type = $("#operations-search").val();
                if (type === "queue") {
                    $("#operations-search-fields").addClass("hidden");
                } else {
                    $("#operations-search-fields").removeClass("hidden");
                    if (type === "id") {
                        $("#input-search-operation-key").text("Object id:");
                    } else if (type === "blockId") {
                        $("#input-search-operation-key").text("Block id:");
                    } else {
                        $("#input-search-operation-key").text("Block hash:");
                    }
                }
            });

            $("#operations-search-btn").click(function () {
                var typeSearch = $("#operations-search").val();
                var key = $("#operations-key").val();

                OPERATION_VIEW.loadOperationView();
                var url = "/api/admin?view=operations";
                if (typeSearch !== "") {
                    url += '&loadBy=' + typeSearch;
                }
                if (key !== "") {
                    url += '&key=' + key;
                }
                window.history.pushState(null, "State Operations", url);
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

                it.find("[did='op-hash']").html(smallHash(op.hash)).attr("href", "/api/admin?view=objects&browse=operation&key=" + op.hash + "&limit=50");
                it.find("[did='op-type']").html(op.type);
                it.find("[did='op-type-link']").attr("href", "/api/admin?view=objects&browse=type&type=" + op.type + "&subtype=all&limit=50");
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
    var originObject;
    var editor = {};
    function objIdFormat(ar) {
        var it = ar[0] + ": <b>";
        for(var k = 1; k < ar.length; k++) {
            if(k > 1) {
                it +=", ";       
            }
            it += ar[k];
        }
        it += "</b>";
        return it;
    }

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
        delete obj.eval;
        originObject = obj;
        editor = new JsonEditor('#json-display', obj);
        editor.load(obj);
    }

    function setObjectsHistoryItem(obj, templateItem) {
        var it = templateItem.clone();

        it.find("[did='object-op-hash']").attr('data-content', obj.opHash).html(smallHash(obj.opHash)).popover();
        it.find("[did='obj-id']").html(objIdFormat(obj.id));
        it.find("[did='obj-date']").html(obj.date);
        // if (obj.objEdit.eval !== undefined) {
        //     it.find("[did='obj-type']").html(obj.objType);
        // }
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

    function setObjectsHistoryItems(searchType, key) {
        var obj = {
            "type": searchType,
            "key": key,
            "limit": $("#limit-field").val(),
            "sort": "DESC"
        };
        $.ajax({
            url: "/api/history", type: "get", data: obj,
            success: function (data) {
                var items = $("#objects-list");
                items.empty();
                var templateItem = $("#objects-history-list-item");
                for (var i = 0; i < data.length; i++) {
                    var object = data[i];
                    items.append(setObjectsHistoryItem(object, templateItem));
                }
                setAmountResults(data.length);
            },
            error: function (xhr, status, error) {
                $("#result").html("ERROR: " + error);
            }
        });
        
    }
    
    function loadSearchTypeByOpType(type) {
        getJsonAction("/api/indices-by-type?type=" + type, function (data) {
            var searchTypes = $("#search-type-list");
            searchTypes.removeClass("hidden");
            let selectVal = searchTypes.val();
            searchTypes.empty();
            searchTypes.append(new Option("all", "all"));
            searchTypes.append(new Option("by id", "id"));
            searchTypes.append(new Option("count", "count"));
            for (var i = 0; i < data.length; i++) {
                var obj = data[i];
                searchTypes.append(new Option("by " + obj.indexId, obj.indexId));
            }
            if(selectVal) {
                searchTypes.val(selectVal);
            }
        });
    }

    function setAmountResults (data) {
        $("#amount-objects").html("Count results: " + data);
    }

    function setObjectsItems (data) {
        var items = $("#objects-list");
        items.empty();
        let templateItem = $("#objects-list-item");
        let type = $("#type-list").val();
        for (var i = 0; i < data.objects.length; i++) {
            let obj = data.objects[i];
            var it = templateItem.clone();
            it.find("[did='edit-object']").click(function () {
                generateJsonFromObject(obj);
            });
            it.find("[did='object-op-hash']").attr('data-content', obj.eval.parentHash).html(smallHash(obj.eval.parentHash)).popover();
            it.find("[did='obj-id']").html(type+": <b>"+obj.id.toString()+"</b>");
            if (obj.comment) {
                it.find("[did='obj-comment']").html(obj.comment);
            } else {
                it.find("[did='comment']").prop("hidden", true);
            }
            it.find("[did='object-json']").html(JSON.stringify(obj, null, 4));
            items.append(it);
        }
        setAmountResults(data.objects.length);
    }

    return {
        generateEditOp: function(op) {
            editor.load(op);
        },
        loadObjectTypes: function() {
            $("#index-list-id").hide();
            getJsonAction("/api/objects?type=sys.operation", function (data) {
                $("#objects-tab").html("Objects (" + data.objects.length + ")");
                for (var i = 0; i < data.objects.length; i++) {
                    let obj = data.objects[i];
                    let objType = obj.id[0];
                    if ($("#type-list [value='" + objType + "']").length == 0) {
                        $("#type-list").append(new Option(objType, objType));
                    }
                    $("#index-op-types").append(new Option(objType, objType));
                }
            }
            );
        },

        loadURLParams: function(url) {
            var browse = url.searchParams.get('browse');
            if (browse !== null) {
                $("#browse-list").val(browse);
                if (browse === "operation") {
                    $("#name-key-field").text("Operation hash:");
                } else {
                    $("#name-key-field").text("User id:");
                }
            }
            var typeValue = url.searchParams.get('type');
            if (typeValue !== null) {
                if($("#type-list [value='"+typeValue+"']").length == 0) {
                    $("#type-list").append(new Option(typeValue, typeValue));
                }
                $("#type-list").val(typeValue);
            }
            var subtypeValue = url.searchParams.get('subtype');
            if (subtypeValue !== null) {
                if($("#search-type-list [value='"+subtypeValue+"']").length == 0) {
                    $("#search-type-list").append(new Option(subtypeValue, subtypeValue));
                }
                $("#search-type-list").val(subtypeValue);
                $("#search-type-list").removeClass("hidden");
            } else {
                $("#search-type-list").addClass("hidden");
            }
            if(browse == "operation" || browse == "userid") {
                $("#type-list-select").addClass("hidden");
            }

            var limitValue = url.searchParams.get('limit');
            if (limitValue !== null) {
                $("#limit-field").val(limitValue);
            }
            var keyValue = url.searchParams.get('key');
            if (keyValue !== null) {
                $("#search-key").val(keyValue);
                $("#search-key-input").removeClass("hidden");
            }
            OBJECTS_VIEW.loadObjectView();
        },

        loadObjectView: function() {
            let browse = $("#browse-list").val();
            let searchType = $("#search-type-list").val();
            let type = $("#type-list").val();
            let key = $("#search-key").val();
            $("#json-editor-div").addClass("hidden");
            $("#stop-edit-btn").addClass("hidden");
            $("#finish-edit-btn").addClass("hidden");
            $("#add-edit-op-btn").addClass("hidden");
            if (browse === "operation") {
                setObjectsHistoryItems("operation", key);
            } else if (browse === "userid") {
                setObjectsHistoryItems("user", key);
            } else if (browse === "history") {
                if(type == "all") {
                    setObjectsHistoryItems("all", null);
                } else {
                    setObjectsHistoryItems("type", type);
                }
            } else if (browse === "type") {
                if (searchType === "count") {
                    getJsonAction("/api/objects-count?type=" + type, function (data) {
                        $("#objects-list").empty();
                        setAmountResults(data.count);
                    });
                } else if (searchType === "all") {
                    var req = {
                        "type": type,
                        "limit": $("#limit-field").val()
                    };
                    $.getJSON("/api/objects", req, function (data) {
                        setObjectsItems(data);
                    });
                } else if (searchType === "id") {
                    var req = {
                        "type": type,
                        "key": key
                    };
                    $.getJSON("/api/objects-by-id", req, function (data) {
                        setObjectsItems(data);
                    });
                } else {
                    var req = {
                        "type": type,
                        "index": $("#search-type-list").val(),
                        "limit": $("#limit-field").val(),
                        "key": key
                    };
                    $.getJSON("/api/objects-by-index", req, function (data) {
                        setObjectsItems(data);
                    });
                }
            }
        },

        onReady: function() {
            $("#browse-list").change(function() {
                var selected = $("#browse-list").val();
                $("#search-key").val("");
                if (selected === "type") {
                    // $("#type-list [value='all']").attr("disabled", "").removeAttr("selected").val("select");
                    loadSearchTypeByOpType();

                    $("#type-list-select").removeClass("hidden");
                    $("#search-type-list").removeClass("hidden");
                    $("#search-key-input").addClass("hidden");
                } else if (selected === "history") {
                    // $("#type-list [value='all']").removeAttr("disabled");
                    // $("#type-list [value='all']").val("all");
                    $("#type-list-select").removeClass("hidden");
                    $("#search-type-list").addClass("hidden");
                    $("#search-key-input").addClass("hidden");
                } else {
                    $("#type-list-select").addClass("hidden");
                    $("#search-key-input").removeClass("hidden");
                    if (selected === "operation") {
                        $("#name-key-field").text("Operation hash:");
                    } else {
                        $("#name-key-field").text("User id:");
                    }
                }
            });

            $("#load-objects-btn").click(function () {
                var browse = $("#browse-list").val();
                var subtype = $("#search-type-list").val();
                var type = $("#type-list").val();
                var key = $("#search-key").val();
                $("#json-editor-div").addClass("hidden");
                $("#stop-edit-btn").addClass("hidden");
                $("#finish-edit-btn").addClass("hidden");
                $("#add-edit-op-btn").addClass("hidden");

                OBJECTS_VIEW.loadObjectView();
                var url = '/api/admin?view=objects&browse=' + browse;
                if ($("#limit-field").val() !== "") {
                    url += '&limit=' + $("#limit-field").val();
                }
                if((browse === "type" || browse === "history") && type && type !== "") {
                    url += '&type=' + type;
                }
                if(browse === "type" && subtype && subtype !== "") {
                    url += '&subtype=' + subtype;
                }
                if(key && key !== "") {
                    url += '&key=' + key;
                }
                window.history.pushState(null, "State Objects",  url);
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
                if( $("#browse-list").val() == "type") {
                    loadSearchTypeByOpType(type);
                }
            });

            $("#search-type-list").change(function() {
                var selectedSearchType = $("#search-type-list").val();
                if (selectedSearchType === "all") {
                    $("#search-key-input").addClass("hidden");
                } else if ( selectedSearchType === "count") {
                    $("#search-key-input").addClass("hidden");
                } else {
                    $("#search-key-input").removeClass("hidden");
                    if (selectedSearchType === "id") {
                        $("#name-key-field").text("Object id:");
                    } else {
                        $("#name-key-field").text( $("#search-type-list").val() + ":");
                    }
                }
            });

        }
    };


}();