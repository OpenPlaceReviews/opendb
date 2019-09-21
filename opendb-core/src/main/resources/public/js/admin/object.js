var OBJECT_STAB = function () {
    return {
        loadSearchTypeByOpType: function(type) {
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
        },
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

                OBJECT_STAB.loadObjectView();
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
                OBJECT_STAB.loadSearchTypeByOpType(type);
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
}();