var FACET = {};
FACET.ui = {};
FACET.util = {};

(function() {
    var Connect         = YAHOO.util.Connect,           Json = YAHOO.lang.JSON,
        Event           = YAHOO.util.Event,          Overlay = YAHOO.widget.Overlay,
        Dom             = YAHOO.util.Dom,           TreeView = YAHOO.widget.TreeView,
        TextNode        = YAHOO.widget.TextNode,    RootNode = YAHOO.widget.RootNode;

    var showOverlayButtonElName     = "show_overlay",       // from main.tag
        overlayDivElName            = "overlay",            // from main.tag
        contentContainerDivElName   = "content_container",  // from main.tag
        facetContainerDivElName     = "facet_container",    // from main.tag
        treeViewDivElName           = "tree_view",          // from main.tag
        facetOptionsDivElName       = "facet_options",
        buttonDownOverlayCSSClass   = "button down-big-overlay",
        buttonUpOverlayCSSClass     = "button up-big-overlay";

    var contentContainer            = Dom.get(contentContainerDivElName),
        contentContainerMinHeight   = parseInt(Dom.getStyle("content_container", "height").replace("px", ""));

    function makeVisible(el) {
        Dom.setStyle(el, "visibility", "visible");
    }

    makeVisible(facetContainerDivElName);

    var facetTreeView  = new TreeView(treeViewDivElName);

    var overlay = new Overlay(overlayDivElName, {
        context: [showOverlayButtonElName, "tl","bl", ["beforeShow", "windowResize"]],
        visible: false
    });
    overlay.render(contentContainer);

    var facetTreeUrl = "";
    FACET.ui.initFacetVars = function(url) {
        facetTreeUrl = url;
    };

    function adjustContentContainerHeight() {
        var oh = parseInt(Dom.getStyle(overlayDivElName, "height").replace("px", "")) + 300;
        var h = (oh > contentContainerMinHeight) ? oh : contentContainerMinHeight;
        Dom.setStyle(contentContainer, "height", h + "px");
    }

    function showOverlay() {
        overlay.show();
        Dom.removeClass(showOverlayButtonElName, buttonDownOverlayCSSClass);
        Dom.addClass(showOverlayButtonElName, buttonUpOverlayCSSClass);
    }

    function hideOverlay() {
        overlay.hide();
        Dom.addClass(showOverlayButtonElName, buttonDownOverlayCSSClass);
        Dom.removeClass(showOverlayButtonElName, buttonUpOverlayCSSClass);
    }

    Event.addListener(showOverlayButtonElName, "click", function(e) {
        if (Dom.hasClass(showOverlayButtonElName, buttonDownOverlayCSSClass)) {
            showOverlay();
        } else {
            hideOverlay();
        }
    }, overlay, true);


    FACET.ui.buildInitialFacetTree = function() {
        Connect.asyncRequest('GET', facetTreeUrl, {
            success: function(o) {
                var result = Json.parse(o.responseText);
                makeVisible(showOverlayButtonElName);
                showOverlay();
                FACET.ui.buildFacetTree(result.response.facets);
            },
            failure: function(o) {
                alert("Could not build facets. " + o);
            }
        });
    };

    FACET.ui.buildFacetTree = function(facets) {
        var i, j, nameNode, valueNode, root = facetTreeView.getRoot();

        facetTreeView.removeChildren(root);

        for(i = 0; i < facets.length; i++) {
            var parent = root;
            if (facets[i].name.indexOf(".") > 0) {
                var facetNameArray = facets[i].name.split(".");
                var facetParentName = facetNameArray[0], facetChildName = facetNameArray[1];
                var parentNode = facetTreeView.getNodeByProperty("label", facetParentName);
                if (parentNode == null) {
                    parentNode = new TextNode(facetParentName, root, false);
                }
                parent = parentNode;
                facets[i].name = facetChildName;
            }

            if (facets[i].values.length > 0 &&
                !(facets[i].values.length == 1 && facets[i].values[0].match(/^null\s\([0-9]+\)$/))) {
                nameNode = new TextNode(facets[i].name, parent, false);

                for(j = 0; j < facets[i].values.length; j++) {
                    valueNode = new TextNode(facets[i].values[j], nameNode, false);
                    valueNode.isLeaf = true;
                }
            }
        }

        facetTreeView.render();
        adjustContentContainerHeight();

        facetTreeView.subscribe("expandComplete", adjustContentContainerHeight);
        facetTreeView.subscribe("collapseComplete", adjustContentContainerHeight);

        facetTreeView.subscribe("clickEvent", function(e) {
            Event.stopEvent(e);
            if (facetTreeView.getRoot() == e.node.parent) {
                return;
            }

            var node = e.node;
            var anchorText = (!(node.parent.parent instanceof RootNode) ? node.parent.parent.label + "." : "")
                + node.parent.label + " : " + node.label.substring(0, node.label.lastIndexOf("(") - 1);

            if (Dom.inDocument("treeNode" + node.index) == false) {
                var anchor = UI.addDomElementChild("a", Dom.get(facetOptionsDivElName),
                    [{ key: "id", value: "treeNode" + node.index }],
                    [{ key: "margin", value: "2px"}, {key : "class", value: "button delete" }]);
                anchor.appendChild(document.createTextNode(anchorText));

                Event.addListener("treeNode" + node.index, "click", function(e) {
                    UI.removeElement("treeNode" + node.index);
                });
            }
        });
    };

    FACET.util.getFacetFilterQueryString = function() {
        var i, facetOptions = {}, domFacets = Dom.get(facetOptionsDivElName).children;
        for (i = 0; i < domFacets.length; i++) {
            if (!Dom.hasClass(domFacets[i], "clearboth")) {
                var t = domFacets[i].innerHTML.split(" : ");
                var o = "\"" + t[1] + "\"";
                facetOptions[t[0]] = facetOptions.hasOwnProperty(t[0]) ? facetOptions[t[0]] + " " + o: o;
            }
        }

        var fqStr = "";
        if (Object.keys(facetOptions).length > 0) {
            for (var key in facetOptions) {
                fqStr += "%2B" + key + ":(" + facetOptions[key].encodeForRequest() + ")";
            }
        }

        return fqStr;
    };

    FACET.ui.buildFacetHTML = function(insertAfterElName) {
        var insertAfterNode = Dom.get(insertAfterElName);
        var el = UI.insertDomElementAfter('div', insertAfterNode, [{ key: 'id', value: facetOptionsDivElName}], []);
        UI.addDomElementChild('div', el, [], [{ key: 'class', value: 'clearboth'}]);
    }

})();