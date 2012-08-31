<%@ taglib prefix="layout" tagdir="/WEB-INF/tags/layout"%>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core"%>
<%@ taglib prefix="fn" uri="http://java.sun.com/jsp/jstl/functions" %>

<layout:main>
    <link rel="stylesheet" type="text/css" href="<c:url value="/static/css/index.css"/>" />

    <h2>Solr Cores</h2>

    <div id="tree_view"></div>

    <form id="create_core_form">
        <div id="create_new_core_div">
            <h4 id="create_new_core_header">Create new core</h4>
            <div class="index_row">
                <label for="new_core_name" id = "new_core_name_label" class="index_label"><span class="red">*</span>Name: </label>
                <input id="new_core_name" type="text" class="index_input"/>
            </div>
            <div class="index_row">
                <label for="instance_dir" id = "instance_dir_label" class="index_label">Instance directory: </label>
                <input id="instance_dir" type="text" class="index_input" value="solr/{name}"/>
            </div>
            <div class="index_row">
                <label for="data_dir" id = "data_dir_label" class="index_label">Data directory: </label>
                <input id="data_dir" type="text" class="index_input" value="solr/{name}/data"/>
            </div>
            <div class="index_row">
                <label for="config_file_name" id = "config_file_name_label" class="index_label">Config file: </label>
                <input id="config_file_name" type="text" value="solrconfig.xml" class="index_input"/>
            </div>
            <div class="index_row">
                <label for="schema_file_name" id = "schema_file_name_label" class="index_label">Schema file: </label>
                <input id="schema_file_name" type="text" value="schema.xml" class="index_input"/>
            </div>

            <div class="buttons index_buttons">
                <a href="#" class="button small" id="create">Create</a>
            </div>
            <div class = "index_row"></div>
        </div>
    </form>

    <script type="text/javascript">
    (function() {
        var Dom = YAHOO.util.Dom,
                Event = YAHOO.util.Event,
                Connect = YAHOO.util.Connect,
                Json = YAHOO.lang.JSON,
                TreeView = YAHOO.widget.TreeView,
                TextNode = YAHOO.widget.TextNode;

        var treeView = new YAHOO.widget.TreeView("tree_view");

        var buildTreeRecurse = function(doc, keyname, parentNode) {
            var key, isObject = Object.prototype.toString.call(doc).match("Object") != null;
            var name = isObject ? keyname : keyname + ": " + doc;
            var nameNode = new TextNode(name, parentNode, false);

            if (!isObject) {
                nameNode.isLeaf = true;
            } else {
                for(key in doc) {
                    buildTreeRecurse(doc[key], key, nameNode);
                }
            }
        };

        var buildTree = function(docs) {
            var i, j, key, nameNode, root = treeView.getRoot();
            treeView.removeChildren(root);

            for(i = 0; i < docs.length; i++) {
                nameNode = new TextNode(docs[i].name, root, false);
                for(key in docs[i]) {
                    buildTreeRecurse(docs[i][key], key, nameNode);
                }
            }
            treeView.render();
            treeView.expandAll();
        };

        Connect.asyncRequest('GET', '<c:url value="/core/info/all" />' , {
            success : function(o) {
                buildTree(Json.parse(o.responseText));
            },
            failure : function (o) {
                alert("Could not retrieve core information.");
            }
        });

        Event.addListener("create", "click", function(e) {
            Event.stopEvent(e);

            var newcoreinfo =  { "corename" : Dom.get("new_core_name").value };

            Connect.initHeader('Content-Type', 'application/json');
            Connect.setDefaultPostHeader('application/json');
            Connect.asyncRequest('POST', '<c:url value="/collection/create" />' , {
                success: function (o) {
                    if (!LWA.ui.alertErrors(o)) {
                        window.location.reload();
                    }
                },
                failure: function (e) {
                    alert("Could not create new collection");
                    console.log(e);
                }
            }, YAHOO.lang.JSON.stringify(newcollectioninfo));
        });

    })();
    </script>
</layout:main>