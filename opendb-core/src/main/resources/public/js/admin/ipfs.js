var IPFS_STAB = function () {
    return {
        loadIpfsStatusData: function() {
            $.getJSON("/api/ipfs/status?full=false", function (data) {
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
        },
        loadFullIpfsStatus: function() {
            $.getJSON("/api/ipfs/status?full=true", function (data) {
                $("#result").html("SUCCESS: " + data);
                $("#amount-missing-ipfs-objects").html(data.missingResources.length);
                $("#amount-db-objects").html(data.amountDBResources);
                $("#amount-unactivated-objects").html(data.deprecatedResources.length);

                for (var i = 0; i < data.missingResources.length; i++) {
                    $("#ipfs-missing-objects").html(data.missingResources[i].hash + " , ");
                }
                for (var i = 0; i < data.deprecatedResources.length; i++) {
                    $("#blockchain-unactivated-images").append(data.deprecatedResources[i].hash + " , ");
                }
            });
        },
        onReady: function() {
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
                    .done(function (data) {$("#result").html("SUCCESS: " + data); loadData(); IPFS_STAB.loadFullIpfsStatus(); })
                    .fail(function (xhr, status, error) { $("#result").html("ERROR: " + error); })
            });

            $("#fix-blc-missing-images-btn").click(function () {
                $.post("/api/ipfs/mgmt/clean-deprecated-ipfs")
                    .done(function (data) {
                        $("#result").html("SUCCESS: " + data);
                        loadData();
                        IPFS_STAB.loadFullIpfsStatus();
                    })
                    .fail(function (xhr, status, error) {
                        $("#result").html("ERROR: " + error);
                    });
            });

            $("#laod-full-stats-btn").click(function () {
                IPFS_STAB.loadFullIpfsStatus();
            });
        }
    };
} ();