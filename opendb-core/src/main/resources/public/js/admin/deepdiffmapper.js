// TODO comment source code link
var DEEP_DIFF_MAPPER = function() {
            return {
                VALUE_CREATED: 'created',
                VALUE_UPDATED: 'updated',
                VALUE_DELETED: 'deleted',
                VALUE_UNCHANGED: 'unchanged',
                map: function(obj1, obj2) {
                    if (this.isFunction(obj1) || this.isFunction(obj2)) {
                        throw 'Invalid argument. Function given, object expected.';
                    }
                    if (this.isValue(obj1) || this.isValue(obj2)) {
                        var opType = this.compareValues(obj1, obj2);
                        if (opType === this.VALUE_UPDATED) {
                            return {
                                editType: opType,
                                old: (obj1 === undefined) ? obj2 : obj1,
                                new: (obj2 === undefined) ? obj1 : obj2
                            }
                        } else if (opType === this.VALUE_CREATED) {
                            return {
                                editType: opType,
                                new: (obj1 === undefined) ? obj2 : obj1
                            };
                        } else if (opType === this.VALUE_DELETED) {
                            return {
                                editType: opType,
                                old: (obj1 === undefined) ? obj2 : obj1
                            }
                        } else {
                            return {
                                editType :opType
                            }
                        }
                    }

                    var diff = {};
                    for (var key in obj1) {
                        if (this.isFunction(obj1[key])) {
                            continue;
                        }

                        var value2 = undefined;
                        if ('undefined' != typeof(obj2[key])) {
                            value2 = obj2[key];
                        }

                        diff[key] = this.map(obj1[key], value2);
                    }
                    for (var key in obj2) {
                        if (this.isFunction(obj2[key]) || ('undefined' != typeof(diff[key]))) {
                            continue;
                        }

                        diff[key] = this.map(undefined, obj2[key]);
                    }

                    return diff;

                },
                compareValues: function(value1, value2) {
                    if (value1 === value2) {
                        return this.VALUE_UNCHANGED;
                    }
                    if (this.isDate(value1) && this.isDate(value2) && value1.getTime() === value2.getTime()) {
                        return this.VALUE_UNCHANGED;
                    }
                    if ('undefined' == typeof(value1)) {
                        return this.VALUE_CREATED;
                    }
                    if ('undefined' == typeof(value2)) {
                        return this.VALUE_DELETED;
                    }

                    return this.VALUE_UPDATED;
                },
                generateChangeCurrentObject: function(path, obj, changeEdit, currentEdit) {
                    if (obj.hasOwnProperty("editType")) {
                        if (obj["editType"] === this.VALUE_CREATED) {
                            var setObj = {
                                set: obj["new"]
                            };
                            changeEdit[path] = setObj;
                        } else if (obj["editType"] === this.VALUE_DELETED) {
                            changeEdit[path] = "delete";
                            currentEdit[path] = obj["old"];
                        } else if (obj["editType"] === this.VALUE_UPDATED) {
                            var setObj = {
                                set: obj["new"]
                            };
                            changeEdit[path] = setObj;
                            currentEdit[path] = obj["old"];
                        }
                    } else {
                        for (var key in obj) {
                            if (key === "version") {
                                changeEdit[key] = "increment";
                            }
                            var mapObj = obj[key];
                            var path1;
                            var value = Number(key);
                            if (!isNaN(value)) {
                                path1 = path === "" ? key : (path + "[" + key + "]");
                            } else {
                                path1 = path === "" ? key : (path + "." + key);
                            }
                            this.generateChangeCurrentObject(path1, mapObj, changeEdit, currentEdit);
                        }
                    }
                },
                generateEditOp: function(obj, changeEdit, currentEdit) {
                    var listEdit = [
                        {
                            id: obj["id"],
                            change: changeEdit,
                            current: currentEdit
                        }
                    ];
                    var op = {
                        type: $("#type-list").val(),
                        edit: listEdit
                    };
                    editor.load(op);
                },
                isFunction: function(obj) {
                    return {}.toString.apply(obj) === '[object Function]';
                },
                isArray: function(obj) {
                    return {}.toString.apply(obj) === '[object Array]';
                },
                isObject: function(obj) {
                    return {}.toString.apply(obj) === '[object Object]';
                },
                isDate: function(obj) {
                    return {}.toString.apply(obj) === '[object Date]';
                },
                isValue: function(obj) {
                    return !this.isObject(obj) && !this.isArray(obj);
                }
            }
}();
