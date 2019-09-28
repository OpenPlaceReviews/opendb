var SETTINGS_VIEW = function () {
    function editPreferenceObject(obj) {
        if (obj === "Map") {
            $("#settings-edit-modal .modal-body #edit-preference-value").val(JSON.stringify(obj.value, null, 4));
        } else {
            $("#settings-edit-modal .modal-body #edit-preference-value").val(obj.value);
        }
        $("#settings-edit-modal .modal-body #settings-type-edit-header").html("Preference type: " + obj.type);
        $("#settings-edit-modal .modal-body #settings-type").html(obj.type);
        $("#settings-edit-modal .modal-body #edit-preference-restart").val(obj.restartIsNeeded.toString());
        $("#settings-edit-modal .modal-header #settings-name").val(obj.id);
        $("#settings-edit-modal .modal-header #settings-edit-header").html("Preference: " + obj.id);
    }
    function showSettings(obj, templateItem) {
        var it = templateItem.clone();
        if (obj.canEdit === true) {
            it.find("[did='edit-settings']").click(function () {
                editPreferenceObject(obj);
            });
        } else {
            it.find("[did='edit-settings']").addClass("hidden");
        }
        it.find("[did='settings-name']").html(obj.id);
        it.find("[did='settings-description']").html(obj.description);
        if (obj.type === "Map") {
            it.find("[did='settings-value-json']").html(JSON.stringify(obj.value, null, 4));
            it.find("[did='settings-value']").addClass("hidden");
        } else {
            it.find("[did='settings-value-json']").addClass("hidden");
            it.find("[did='settings-value']").html(obj.value);
        }
        if (obj.restartIsNeeded === true) {
            it.find("[did='settings-restart']").removeClass("hidden");
        }
        return it;
    }

    return {
        loadConfiguration: function() {
            $.getJSON("/api/mgmt/config", function (data) {
                var items = $("#settings-result-list");
                items.empty();
                var templateItem = $("#settings-list-item");
                for (var i = 0; i < data.length; i++) {
                    obj = data[i];
                    items.append(showSettings(obj, templateItem));
                    
                }
            });
        },
        onReady: function() {
            $("#settingsCheckbox").change(function () {
                SETTINGS_VIEW.loadConfiguration();
            });
            $("#save-new-value-for-settings-btn").click(function () {
                var obj = {
                    key: $("#settings-name").val(),
                    value: $("#edit-preference-value").val(),
                    type: $("#settings-type").text(),
                };
                // TODO use generic error handling
                $.post("/api/mgmt/config", obj)
                    .done(function (data) {
                        $("#result").html("SUCCESS: " + data);
                        SETTINGS_VIEW.loadConfiguration();
                    })
                    .fail(function (xhr, status, error) {
                        $("#result").html("ERROR: " + error);
                    });

                $("#settings-edit-modal .close").click();
            });
        }
    };

    
} ();

var METRIC_VIEW = function () {
    return {
        loadMetricsData: function() {
            $.getJSON("/api/metrics", function (data) {
                metricsData = data.metrics;
                setMetricsDataToTable();
            });
        },
        onReady: function() {
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
        }
    };

    function setMetricsDataToTable() {
        var gid = 0;
        if ($("#metrics-a").prop("checked")) {
            gid = 1;
        }
        if ($("#metrics-b").prop("checked")) {
            gid = 2;
        }

        var table = $("#main-metrics-table");
        table.empty();
        var template = $("#metrics-template");

        for (var i = 0; i < metricsData.length; i++) {
            let item = metricsData[i];
            var newTemplate = template.clone()
                .appendTo(table)
                .show();
            var lid = item.id;
            if (lid.length > 50) {
                lid = lid.substring(0, 50);
            }
            newTemplate.find("[did='id']").html(lid);
            newTemplate.find("[did='count']").html(item.count[gid]);
            newTemplate.find("[did='total']").html(item.totalSec[gid]);
            newTemplate.find("[did='average']").html(item.avgMs[gid]);
            if (item.avgMs > 0) {
                newTemplate.find("[did='throughput']").html(Number(1000 / item.avgMs[gid]).toFixed(2));
            } else {
                newTemplate.find("[did='throughput']").html("-");
            }
        }
    }
} ();

var IPFS_VIEW = function () {
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
        }
    };

    function loadFullIpfsStatus() {
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
    }
} ();