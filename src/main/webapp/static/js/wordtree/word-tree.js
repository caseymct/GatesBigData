var WORDTREE = {};

(function() {
    WORDTREE.treeObj = { paper : null, rtree : null, ltree : null, altQueryRaphaelElements: [] };

    var nodeID    = -1,
        searchUrl = "",
        maxLevel  = 0;

    var NAME_KEY             = 'name',           COUNT_KEY            = 'count',
        CHILDREN_KEY         = 'children',       SENTENCE_KEY         = 'sentence',
        PAPER_KEY            = 'paper',          PREFIXES_KEY         = 'prefixes',
        SUFFIXES_KEY         = 'suffixes',       ALT_QUERIES_KEY      = 'altQueries',
        SEARCH_URL_KEY       = 'searchUrl',
    
        FONT_MAX_SIZE        = 24,               FONT_MIN_SIZE        = 10,
        TOP_X_ADJUST_DEFAULT = 0,                TOP_Y_ADJUST_DEFAULT = 0,
        TEXT_X_PADDING       = 5,                TEXT_Y_PADDING       = 8,
        DESC_TEXT_FONT_SIZE  = 14;


    function size(count, level){
        return Math.min(FONT_MAX_SIZE, Math.max(FONT_MIN_SIZE, (12 + count)/(Math.log(level + 1))));
    }

    function addAltQueries(paper, altQueries) {
        if (altQueries == undefined || altQueries.length == 0) return [];

        var i, descText = "";
        for(i = 0; i < altQueries.length; i++) {
            descText += altQueries[i][NAME_KEY] + " (" + altQueries[i][COUNT_KEY] + ")";
            if (i < altQueries.length - 1) {
                descText += "\n";
            }
        }

        var suffixRootNode = paper.getById(WordTree.SUFFIX_ROOT_NODE_RAPHAEL_EL_ID);
        var prefixRootNode = paper.getById(WordTree.PREFIX_ROOT_NODE_RAPHAEL_EL_ID);
        var x = (prefixRootNode.getBBox()['x'] + suffixRootNode.getBBox()['x2'])/2;
        var y = prefixRootNode.getBBox()['y2'] + 10;

        var descTextEl = paper.text(x + TEXT_X_PADDING, y + TEXT_Y_PADDING, "Query appears as:");
        descTextEl.attr("font-size", DESC_TEXT_FONT_SIZE).attr("font-weight", "bold");

        var altQueriesY = descTextEl.getBBox().y2 + DESC_TEXT_FONT_SIZE;
        var altQueriesEl = paper.text(x + TEXT_X_PADDING, altQueriesY, descText);
        var altQueriesBBox = altQueriesEl.getBBox(), descTextBBox = descTextEl.getBBox();

        altQueriesEl.attr("font-size", "12").attr("y", altQueriesY + altQueriesBBox.height/2);

        var rectW = Math.max(altQueriesBBox.width, descTextBBox.width) + TEXT_X_PADDING*2;
        var rectH = (altQueriesEl.getBBox().y2 - descTextBBox.y) + TEXT_Y_PADDING;

        var rect = paper.rect(Math.min(altQueriesBBox.x, descTextBBox.x) - TEXT_X_PADDING, y, rectW, rectH, 8).attr({
            fill : 'white',
            stroke : 'lightgray'
        });

        return [descTextEl, altQueriesEl, rect];
    }

    function add(json, pid, tree, level, altQueries) {
        if (level > maxLevel) {
            maxLevel = level;
        }

        nodeID++;
        var name     = json[NAME_KEY];
        var count    = json[COUNT_KEY];
        var text     = name + " (" + count + ") ";
        var tgt      = (nodeID == 0 && altQueries.length > 0) ? JSON.stringify(altQueries) : json[SENTENCE_KEY];
        tree.add(nodeID, pid, text, size(count, level), tgt);

        var children = json[CHILDREN_KEY];
        if (children != undefined) {
            var parentID = nodeID;
            for(var i = 0; i < children.length; i++) {
                add(children[i], parentID, tree, level + 1, altQueries);
            }
        }
    }

    function redraw(rtree, ltree) {
        rtree.UpdateTree();
        ltree.UpdateTree();
        rtree.collapseAboveLevel();
        ltree.collapseAboveLevel();
    }

    function createTree(words, treeOptions, altQueries) {
        nodeID = -1;
        var tree = new WordTree(treeOptions);
        add(words, -1, tree, 0, altQueries);
        return tree;
    }

    function getTreeOptions(orientation, paper){
        return {
            confOptions : {
                iRootOrientation : orientation,
                collapseAboveLevel : (maxLevel > 2) ? 1 : maxLevel,
                topXAdjustment : TOP_Y_ADJUST_DEFAULT,
                topYAdjustment : TOP_X_ADJUST_DEFAULT
            },
            settings : {
                paper     : paper,
                searchUrl : searchUrl
            }
        };
    }

    function setJustDragged(paper) {
        WordTree.setJustDraggedStatus(paper, true);
    }

    function getAllElements() {
        return WORDTREE.treeObj.ltree.paperSet.items.concat(WORDTREE.treeObj.rtree.paperSet.items);
    }

    var startAll = function () {
        var allElements = getAllElements();//this.paper.canvas.parentNode.id);

        for(var i = 0; i < allElements.length; i++){
            var el = allElements[i], bbox = el.getBBox(), isPath = (allElements[i].type == "path");

            el.ox = isPath ? bbox.x : el.attr('x');
            el.oy = isPath ? bbox.y : el.attr('y');
        }
    };
    var moveAll = function (dx, dy) {
        var allElements = getAllElements();//this.paper.canvas.parentNode.id);
        for(var i = 0; i < allElements.length; i++) {
            var el = allElements[i], bbox = el.getBBox(), isPath = (allElements[i].type == "path");
            if (!isPath) {
                el.attr({x: el.ox + dx, y: el.oy + dy});
            } else {
                el.translate(el.ox - bbox.x + dx, el.oy - bbox.y + dy);
            }
        }
        setJustDragged(this.paper);
    };
    var upAll = function () { };

    function getSelectedElementTransform() {
        var ltree = WORDTREE.treeObj.ltree,
            rtree = WORDTREE.treeObj.rtree,
            lsel = ltree.iSelectedNode,
            rsel = rtree.iSelectedNode;

        if (lsel == -1 && rsel == -1) return null;

        var selNode  = (lsel != -1) ? ltree.getNodeFromNodeId(lsel) : rtree.getNodeFromNodeId(rsel);
        var rootNode = (lsel != -1) ? ltree.getNodeFromNodeId(0)    : rtree.getNodeFromNodeId(0);

        return { dx : rootNode.XPosition - selNode.raphaelElements.boundingRect.attr('x'),
                 dy : rootNode.YPosition - selNode.raphaelElements.boundingRect.attr('y') };
    }

    WORDTREE.recenterOnSelected = function() {
        var allElements = getAllElements();
        var xform = getSelectedElementTransform();
        if (xform == null) return;

        for(var i = 0; i < allElements.length; i++) {
            var el = allElements[i], isPath = (allElements[i].type == "path");
            if (!isPath) {
                el.attr({x: el.attr('x') + xform.dx, y: el.attr('y') + xform.dy} );
            } else {
                el.translate(xform.dx, xform.dy);
            }
        }
    };

    WORDTREE.makeWordTrees = function(conf) {
        var paper = conf[PAPER_KEY], altQueries = conf[ALT_QUERIES_KEY];
        searchUrl = conf[SEARCH_URL_KEY];

        var rtree = createTree(conf[SUFFIXES_KEY], getTreeOptions(WordTree.RO_LEFT,  paper), altQueries);
        var ltree = createTree(conf[PREFIXES_KEY], getTreeOptions(WordTree.RO_RIGHT, paper), altQueries);
        rtree.setOppositeTree(ltree);
        ltree.setOppositeTree(rtree);

        redraw(rtree, ltree);

        ltree.paperSet.drag(moveAll, startAll, upAll);
        rtree.paperSet.drag(moveAll, startAll, upAll);

        WORDTREE.treeObj = {
            paper : paper,
            rtree : rtree,
            ltree : ltree
        };
    };

    WORDTREE.selectNode = function(whichTree, nodeId) {
        var tree = whichTree == 'R' ? WORDTREE.treeObj.rtree : WORDTREE.treeObj.ltree;
        tree.expandParents(nodeId);
        tree.unhighlightNode();
        tree.selectNode(nodeId);
    };



    function rootNodeIndex(foundNodes) {
        for(var i = 0; i < foundNodes.length; i++) {
            if (foundNodes[i].pid == -1) return i;
        }
        return -1;
    }

    function hasRootNode(foundNodes) {
        return rootNodeIndex(foundNodes) >= 0;
    }

    WORDTREE.search = function(searchStr, searchFromSel) {
        var ltree = WORDTREE.treeObj.ltree,
            rtree = WORDTREE.treeObj.rtree;

        if (searchFromSel && ltree.iSelectedNode == -1 && rtree.iSelectedNode == -1) {
            searchFromSel = false;
        }
        var foundNodes = [];
        if (!searchFromSel || rtree.iSelectedNode != -1) {
            foundNodes = rtree.search(searchStr, searchFromSel);
        }

        if (!searchFromSel || ltree.iSelectedNode != -1) {
            var ltreeNodes = ltree.search(searchStr, searchFromSel);
            if (hasRootNode(foundNodes) && hasRootNode(ltreeNodes)) {
                ltreeNodes.pop(rootNodeIndex(ltreeNodes));
            }
            foundNodes = foundNodes.concat(ltreeNodes);
        }
        return foundNodes;
    };
})();