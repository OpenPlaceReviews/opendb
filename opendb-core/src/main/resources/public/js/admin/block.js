var BLOCK_STAB = function () {
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
                BLOCK_STAB.processBlocksResult(data);
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

                BLOCK_STAB.loadBlockView();
                window.history.pushState(null, "State Blocks", "/api/admin?view=blocks&search=" + type+ "&hash=" + $("#search-block-field").val() + "&limit=" + $("#block-limit-value").val());
            });
        }
    }
}();