<%@ taglib prefix="layout" tagdir="/WEB-INF/tags/layout"%>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core"%>
<%@ taglib prefix="fn" uri="http://java.sun.com/jsp/jstl/functions" %>

<layout:main>
    <link rel="stylesheet" type="text/css" href="<c:url value="/static/css/datasource.css"/>" />

    <h2 id = "collection_header"></h2>

    <form id="datasource_form">

        <div class="row" id="webdatasource_header">Create new datasource:</div>

        <div id="webtab">
            <div class = "row">
                <label for="name"><span class = "red">*</span>Name: </label>
                <input id="name" type = "text"/>
            </div>
            <div class = "row">
                <label for="url"><span class = "red">*</span>Url: </label>
                <input id="url" type = "text"/>
            </div>
            <div class = "row">
                <label for="exclude_paths">Exclude paths: </label>
                <input id="exclude_paths" type = "text"/>
            </div>
            <div class = "row">
                <label for="crawler">Crawler: </label>
                <select id="crawler">
                    <option value="lucid.aperture">lucid.aperture</option>
                </select>
            </div>
            <div class = "row">
                <label for="add_failed_docs">Add failed docs: </label>
                <input id="add_failed_docs" type = "checkbox"/>
            </div>
            <div class = "row">
                <label for="verify_access">Verify access: </label>
                <input id="verify_access" type = "checkbox" selected/>
            </div>
            <div class = "row">
                <label for="indexing">Indexing: </label>
                <input id="indexing" type = "checkbox" selected/>
            </div>
            <div class = "row">
                <label for="ignore_robots">Ignore robots: </label>
                <input id="ignore_robots" type = "checkbox"/>
            </div>
            <div class = "row">
                <label for="log_extra_detail">Log extra detail: </label>
                <input id="log_extra_detail" type = "checkbox"/>
            </div>
            <div class="buttons">
                <a href="#" class="button small" id="create_or_update">Create</a>
                <a href="#" class="button small" id="cancel">Cancel</a>
            </div>
            <div class = "row"></div>
        </div>
    </form>
    <!--

    exclude_paths: [ ],
    mapping: {},
    collection: "suresh",
    log_extra_detail: false,
    type: "web",
    crawler: "lucid.aperture",
    proxy_username: "linuxproxy",
    id: 10,
    name: "epic",
    parsing: true,
    commit_within: 900000,
    add_failed_docs: false,
    crawl_depth: 5,
    caching: false,
    proxy_password: "arg3ntina",
    commit_on_finish: true,
    max_bytes: 10485760,
    include_paths: [ ],
    proxy_host: "192.168.125.50",
    url: "http://epic.cs.colorado.edu/",
    bounds: "tree",
    proxy_port: 8080,
    category: "Web",
    fail_unsupported_file_types: false,
    warn_unknown_mime_types: false,
    auth: [ ],
    max_docs: -1
    -->
    <script type="text/javascript">
        (function() {
            var Dom     = YAHOO.util.Dom,
                Event   = YAHOO.util.Event,
                Connect = YAHOO.util.Connect,
                Json    = YAHOO.lang.JSON;

            var url = window.location.href.split('/');
            var datasourceId = url[url.indexOf("datasource") + 1];
            var createNew = (datasourceId == -1);
            var collectionName = url[url.indexOf("collection") + 1];
            var urlParams = "?collection=" + collectionName + "&datasourceId=" + datasourceId;

            Dom.get("webdatasource_header").innerHTML = createNew ? "Create new web datasource" :
                    "Edit details for datasource";

            Dom.get("collection_header").innerHTML = "Collection " + collectionName;

            if (!createNew) {
                Connect.asyncRequest('GET', '<c:url value="/datasource/datasourcedetails" />' + urlParams, {
                    success:function(o) {
                        var result = Json.parse(o.responseText);

                        for(var key in result) {
                            var el = Dom.get(key);

                            if (el != null) {
                                if (el.type == "checkbox") {
                                    el.checked = result[key];
                                } else if (el.type == "text") {
                                    el.value = result[key];
                                } else if (el.type == "select-one") {

                                }
                            }
                        }

                    },
                    failure: function(o) {
                        LWA.ui.alertErrors(o);
                    }
                });

                Dom.get("create_or_update").innerHTML = "Update";
            }

            Event.addListener("create", "click", function (e) {
                Event.stopEvent(e);

                var newdatasourceinfo = {
                    'collectionName' : collectionName,
                    'properties' : { "name" : Dom.get("datasource_name").value,
                        "url" : Dom.get("datasource_url").value,
                        "crawler" : "lucid.aperture",
                        "crawl_depth" : 5,
                        "type" : "web" }
                };

                Connect.initHeader('Content-Type', 'application/json');
                Connect.setDefaultPostHeader('application/json');
                Connect.asyncRequest('POST', '<c:url value="/datasource/create" />', {
                    success:function(o) {
                        var result = Json.parse(o.responseText);
                        if (result.hasOwnProperty("errors")) {
                            var errmsg = "Error message : " + result.errors[0].message + "\n" +
                                    "Error key : " + result.errors[0].key + "\n" +
                                    "Error code : " + result.errors[0].code;
                            alert(errmsg);
                        } else {
                            window.location.reload();
                        }
                    },
                    failure:function(e) {
                        alert("Problem encountered adding data: " + e.statusText);
                    }
                }, YAHOO.lang.JSON.stringify(newdatasourceinfo));
            });

        })();

    </script>

</layout:main>