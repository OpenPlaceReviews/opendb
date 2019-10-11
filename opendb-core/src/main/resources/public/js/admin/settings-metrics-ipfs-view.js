var SETTINGS_VIEW = function () {
    function editPreferenceObject(obj) {
        if (obj.type === "Map" || obj.type === "TreeMap") {
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

    function removePreferenceObject(pref) {
        $.ajax({
            url: '/api/mgmt/config/remove?key=' + pref.id,
            type: 'DELETE',
            success: function(data) {
                done(data, true);
                loadAddPossibilityForTab();
                let familyName = $("#settings-pills").children(".active").first().text();
                if (familyName === "DB Indexes") {
                    alert("Please run Bot: Update Indexes")
                }
            }
        });

    }

    function renderSettingItem(obj, templateItem) {
        var it = templateItem.clone();
        if (obj.canEdit === true) {
            it.find("[did='edit-settings']").removeClass("hidden").click(function () {
                editPreferenceObject(obj);
            });
        } else {
            it.find("[did='edit-settings']").addClass("hidden");
        }
        if (obj.family.deletable) {
            it.find("[did='remove-settings']").removeClass("hidden").click(function () {
                removePreferenceObject(obj);
            });
        } else {
            it.find("[did='remove-settings']").addClass("hidden");
        }
        it.find("[did='settings-name']").html(obj.id);
        it.find("[did='settings-description']").html(obj.description);
        it.find("[did='settings-source']").html(obj.source);
        if (obj.type === "Map" || obj.type === "TreeMap") {
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

    function loadSettingsFamily() {
        var data = SETTINGS_VIEW.settingsData;
        var dt = {};
        for (var i = 0; i < data.length; i++) {
            let name = data[i].family.name;
            if (!(name in dt)) {
                dt[name] = data[i].family.prefix;
            }
        }
        dt["All"] = null;
        let pills = $("#settings-pills");
        pills.empty();
        var active = true;
        for (var nm in dt) {
            let link = $("<li id='settings_" + nm.replace(/\s/g, '') +"'>");
            if(active) {
                link.addClass("active");
                active = false;
            }
            link.append($("<a>").text(nm));
            pills.append(link);
            link.click(function () {
                let allpills = $("#settings-pills").children();
                allpills.removeClass("active");
                link.addClass("active");
                displaySettings();
            });
        }
    }

    function displaySettings() {
        var data = SETTINGS_VIEW.settingsData;
        var items = $("#settings-result-list");
        let familyName = $("#settings-pills").children(".active").first().text();
        items.empty();
        var templateItem = $("#settings-list-item");
        for (var i = 0; i < data.length; i++) {
            var obj = data[i];
            if(familyName === "All" || obj.family.name === familyName) {
                items.append(renderSettingItem(obj, templateItem));
            }
        }
    }
    
    function addNewPreference() {
        var obj = {
            family: $("#family-name").html(),
            key:$("#add-preference-key").val(),
            value:$("#add-preference-value").val()
        };

        postActionWithoutFailParam('/api/mgmt/config/new', obj, function (data) {
                done(data, true);
                loadAddPossibilityForTab();
                let familyName = $("#settings-pills").children(".active").first().text();
                if (familyName === "DB Indexes") {
                    alert("Please run Bot: Update Indexes")
                }
            }, false);
    }

    function loadAddPossibilityForTab() {
        let familyName = $("#settings-pills").children(".active").first().text();
        if (familyName === "All") {
            $("#add-settings-btn").attr('disabled', 'disabled');
        } else {
            var data = SETTINGS_VIEW.settingsData;
            for (var i = 0; i < data.length; i++) {
                var obj = data[i];
                if (obj.family.name === familyName) {
                    if (obj.family.editable) {
                        $("#add-settings-btn").removeAttr('disabled');
                    } else {
                        $("#add-settings-btn").attr('disabled', 'disabled');
                    }
                    break;
                }
            }
        }
        displaySettings();
        return familyName;
    }

    return {
        settingsData : [],
        loadURLParams: function() {
            var url = new URL(window.location.href);
            if (window.location.href.toLowerCase().indexOf('api/admin?view=settings') > 1) {
                var activeTab = url.searchParams.get('tab');
                if (activeTab) {
                    var test = $("#settings_" + activeTab);
                    let allpills = $("#settings-pills").children();
                    allpills.removeClass("active");
                    if (test) {
                        test.addClass("active");
                        loadAddPossibilityForTab();
                    }
                }
            }
        },
        loadConfiguration: function() {
            getJsonAction("/api/mgmt/config", function (data) {
                SETTINGS_VIEW.settingsData = data;
                loadSettingsFamily();
                displaySettings();
                SETTINGS_VIEW.loadURLParams();
            });
        },
         onReady: function() {
            $("#refresh-settings-btn").click(function() {
                SETTINGS_VIEW.loadConfiguration();
            });
            $("#save-new-value-for-settings-btn").click(function () {
                var obj = {
                    key: $("#settings-name").val(),
                    value: $("#edit-preference-value").val(),
                    type: $("#settings-type").text()
                };

                postActionWithoutFailParam("/api/mgmt/config", obj,
                    function(data) {
                        done(data, false);
                        SETTINGS_VIEW.loadConfiguration();
                    }, false
                );

                $("#settings-edit-modal .close").click();
            });
            
            $("#settings-pills").click(function () {
                let familyName = loadAddPossibilityForTab();
                var url = '/api/admin?view=settings&tab=' + familyName.replace(/\s/g, '');
                window.history.pushState(null, "State Settings",  url);
            });

            $("#add-settings-btn").click(function () {
                let familyName = $("#settings-pills").children(".active").first().text();
                $("#family-name").html(familyName);
                var data = SETTINGS_VIEW.settingsData;
                for (var i = 0; i < data.length; i++) {
                    var obj = data[i];
                    if (obj.family.name === familyName) {
                        $("#add-preference-key").val(obj.family.prefix + ".");
                        break;
                    }
                }
            });
            
            $("#add-new-settings-btn").click(function () {
                $("#settings-add-modal .close").click();
                addNewPreference();
            });
        }
    };

    
} ();

var METRIC_VIEW = function () {
    var MetricEnum = {
        ALL : 0,
        GROUP_A: 1,
        GROUP_B: 2,
        properties: {
            0: "All",
            1: "Group-A",
            2: "Group-B"
        }
    };

    function loadMetricsFamily() {
        let dt = MetricEnum.properties;
        let pills = $("#metrics-pills");
        pills.empty();
        var active = true;
        for (let nm in dt) {
            let link = $("<li>");
            if(active) {
                link.addClass("active");
                active = false;
            }
            link.append($("<a>").text(dt[nm]));
            pills.append(link);
            link.click(function () {
                let allpills = $("#metrics-pills").children();
                allpills.removeClass("active");
                link.addClass("active");
                setMetricsDataToTable();
            });
        }
    }

    function getActiveId() {
        var gid = 0;
        let name = $("#metrics-pills").children(".active").first().text();
        if (MetricEnum.properties[MetricEnum.GROUP_A] === name) {
            gid = 1;
        }
        if (MetricEnum.properties[MetricEnum.GROUP_B] === name) {
            gid = 2;
        }
        return gid;
    }

    function setMetricsDataToTable() {
        var gid = getActiveId();
        if(gid == 0) {
            $("#reset-metrics-btn").attr('disabled', 'disabled');
        } else {
            $("#reset-metrics-btn").removeAttr('disabled');
        }
        var table = $("#main-metrics-table");
        table.empty();
        var template = $("#metrics-template");

        for (var i = 0; i < METRIC_VIEW.metricsData.length; i++) {
            let item = METRIC_VIEW.metricsData[i];
            var newTemplate = template.clone()
                .appendTo(table)
                .removeClass("hidden")
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

    return {
        metricsData: [],
        loadMetricsData: function() {
            getJsonAction("/api/metrics", function (data) {
                METRIC_VIEW.metricsData = data.metrics;
                loadMetricsFamily();
                setMetricsDataToTable();
            });
        },
        onReady: function() {
            $("#reset-metrics-btn").click(function(){
                postActionWithoutFailParam("/api/metrics-reset?cnt="+ getActiveId(), {},
                    function(data) {
                        METRIC_VIEW.metricsData = data.metrics;
                        setMetricsDataToTable();
                }, true);
            });

            $("#refresh-metrics-btn").click(function(){
                getAction("/api/metrics", {},
                    function(data){
                        METRIC_VIEW.metricsData = data.metrics;
                        setMetricsDataToTable();
                    }, true);
            });

        }
    };

} ();

var IPFS_VIEW = function () {

    function loadFullIpfsStatus() {
        getJsonAction("/api/ipfs/status?full=true", function (data) {
            done(data, false);
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
    };

    return {
        loadIpfsStatusData: function() {
            getJsonAction("/api/ipfs/status?full=false", function (data) {
                $("#ipfs-status").html(data.status);
                $("#ipfs-peer-id").html(data.peerId);
                $("#ipfs-version").html(data.version);
                $("#ipfs-gateway").html(data.gateway);
                $("#ipfs-api").html(data.api);
                if(data.addresses) {
                    $("#ipfs-addresses").html(data.addresses.toString());
                }
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
                        $("#result-hash").removeClass("hidden");
                        $("#result-add-image").html(data.toString());
                        loadData();
                    },
                    error: function (xhr, status, error) {
                        $("#result-add-image").html("ERROR: " + error);
                    }
                });
            });

            $("#refresh-ipfs-btn").click(function() {
                IPFS_VIEW.loadIpfsStatusData();
            });

            $("#get-image-btn").click(function () {
                $("#image-link").attr("href", "/api/ipfs/image?hash=" + $("#get-image").val());
                $("#image-link").click();
            });

            $("#fix-ipfs-missing-images-btn").click(function () {
                postActionWithoutFailParam("/api/ipfs/mgmt/ipfs-maintenance", {},
                    function (data) {
                        done(data, true);
                        loadFullIpfsStatus();
                }, false)
            });

            $("#fix-blc-missing-images-btn").click(function () {
                postActionWithoutFailParam("/api/ipfs/mgmt/clean-deprecated-ipfs", {},
                    function (data) {
                        done(data, true);
                        loadFullIpfsStatus();
                    });
            });

            $("#load-full-stats-btn").click(function () {
                loadFullIpfsStatus();
            });

            $(document).on('change', '.btn-file :file', function() {
                var input = $(this),
                    label = input.val().replace(/\\/g, '/').replace(/.*\//, '');
                input.trigger('fileselect', [label]);
            });

            $('.btn-file :file').on('fileselect', function(event, label) {

                var input = $(this).parents('.input-group').find(':text'),
                    log = label;

                if( input.length ) {
                    input.val(log);
                } else {
                    if( log ) alert(log);
                }

            });
            function readURL(input) {
                if (input.files && input.files[0]) {
                    var reader = new FileReader();

                    reader.onload = function (e) {
                        $('#img-upload').attr('src', e.target.result);
                    };

                    reader.readAsDataURL(input.files[0]);
                }
            }

            $("#image-file").change(function(){
                readURL(this);
            });
        }
    };

} ();