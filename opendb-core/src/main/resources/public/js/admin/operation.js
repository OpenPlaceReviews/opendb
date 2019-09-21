var OPERATION_STAB = function () {
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

                OPERATION_STAB.loadOperationView();
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