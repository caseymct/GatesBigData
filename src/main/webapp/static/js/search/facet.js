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
        treeViewDivElName           = "tree_view",          // from main.tag
        buttonDownOverlayCSSClass   = "button down-big-overlay",
        buttonUpOverlayCSSClass     = "button up-big-overlay",

        contentContainer            = Dom.get(contentContainerDivElName),
        contentContainerMinHeight   = parseInt(Dom.getStyle("content_container", "height").replace("px", "")),

        facetTreeUrl                = "",
        responseFacetKey            = "response",
        solrResponseFacetKey        = "facet_counts";

    FACET.allFacets = {};

    function getFacetOptionDivId(nodeIndex)     { return "treeNodeDiv" + nodeIndex; }
    function getFacetOptionAnchorId(nodeIndex)  { return "treeNode" + nodeIndex; }


    UI.changeFacetContainerVisibility(true);

    var facetTreeView  = new TreeView(treeViewDivElName);

    var overlay = new Overlay(overlayDivElName, {
        context: [showOverlayButtonElName, "tl","bl", ["beforeShow", "windowResize"]],
        visible: false
    });
    overlay.render(contentContainer);


    FACET.ui.init = function(names) {
        facetTreeUrl = names[UI.FACET_URL_KEY];
        buildHTML(names[UI.FACET.INSERT_FACET_HTML_AFTER_EL_NAME_KEY]);
        FACET.ui.buildInitialFacetTree();
    };

    function buildHTML(insertAfterElName) {
        var insertAfterNode = Dom.get(insertAfterElName);
        var el = UI.insertDomElementAfter('div', insertAfterNode, { id: UI.FACET.FACET_OPTIONS_DIV_EL_NAME }, null);
        UI.addClearBothDiv(el);
    }

    function adjustContentContainerHeight() {
        var oh = parseInt(Dom.getStyle(overlayDivElName, "height").replace("px", "")) + 300;
        UI.setStyleOnElement(contentContainer, 'height', Math.max(oh, contentContainerMinHeight) + "px");
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
                FACET.allFacets = result[responseFacetKey][solrResponseFacetKey];

                UI.changeVisibility(showOverlayButtonElName, true);
                showOverlay();
                buildFullFacetTree();
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
            var name = facets[i]['name'], values = facets[i]['values'];

            if (name.indexOf(".") > 0) {
                var nameArr = name.split(".");
                var facetParentName = nameArr[0], facetChildName = nameArr[1];
                var parentNode = facetTreeView.getNodeByProperty("label", facetParentName);
                if (parentNode == null) {
                    parentNode = new TextNode(facetParentName, root, false);
                }
                parent = parentNode;
                facets[i]['name'] = facetChildName;
            }

            if (values.length > 0 && !(values.length == 1 && values[0].match(/^null\s\([0-9]+\)$/))) {
                nameNode = new TextNode(facets[i]['name'], parent, false);

                for(j = 0; j < values.length; j++) {
                    valueNode = new TextNode(facets[i]['values'][j], nameNode, false);
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
            if (facetTreeView.getRoot() == e.node.parent) return;

            var node = e.node;
            var fieldName  = (!(node.parent.parent instanceof RootNode) ? node.parent.parent.label + "." : "") + node.parent.label;
            var fieldValue = node.label.substring(0, node.label.lastIndexOf("(") - 1);

            var facetOptionDivId = getFacetOptionDivId(node.index);

            if (Dom.inDocument(facetOptionDivId) == false) {
                UI.addDeleteButtonForIE(Dom.get(UI.FACET.FACET_OPTIONS_DIV_EL_NAME), facetOptionDivId,
                    fieldName, fieldValue, rebuild);
            }
        });
    };

    function rebuild() {
        if (UI.getFiltersText(UI.FACET.FACET_OPTIONS_DIV_EL_NAME).length == 0) {
            buildFullFacetTree();
        }
    }
    function buildFullFacetTree() {
        if (FACET.allFacets != undefined && FACET.allFacets != null) {
            FACET.ui.buildFacetTree(FACET.allFacets);
        }
    }

})();