var WORDTREE = {};

(function() {
    WORDTREE.treeObj = [];

    var nodeID          = -1,
        viewDocUrl      = "",
        maxLevel        = 0,
        graphElHeight   = 0,
        graphElToIdxMap = {};

    var NAME_KEY        = "name",
        COUNT_KEY       = "count",
        CHILDREN_KEY    = "children",
        SOLR_IDS_KEY    = "solrIds",

        FONT_MAX_SIZE         = 24,
        FONT_MIN_SIZE         = 10,
        TOP_X_ADJUST_DEFAULT  = 100,
        TOP_Y_ADJUST_DEFAULT  = 500,
        TEXT_X_PADDING        = 5,
        TEXT_Y_PADDING        = 8,
        DESC_TEXT_FONT_SIZE   = 14;


    function size(count, level){
        return Math.min(FONT_MAX_SIZE, Math.max(FONT_MIN_SIZE, (12 + count)/(Math.log(level + 1))));
    }

    function addAltQueries(paper, altQueries) {
        if (altQueries == undefined) return [];

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
            fill : 'none',
            stroke : 'lightgray'
        });

        return [descTextEl, altQueriesEl, rect];
    }

    function add(json, pid, tree, level) {
        if (level > maxLevel) {
            maxLevel = level;
        }

        nodeID++;
        var name    = json[NAME_KEY];
        var count   = json[COUNT_KEY];
        var solrIds = json[SOLR_IDS_KEY];
        var text    = name + " (" + count + ") ";
        tree.add(nodeID, pid, text, size(count, level), solrIds == undefined ? [] : solrIds, null);

        var children = json[CHILDREN_KEY];
        if (children != undefined) {
            var parentID = nodeID;
            for(var i = 0; i < children.length; i++) {
                add(children[i], parentID, tree, level + 1);
            }
        }
    }

    function redraw(rtree, ltree) {
        rtree.UpdateTree();
        ltree.UpdateTree();
        rtree.config.topXAdjustment = 0;
        ltree.config.topXAdjustment = 0;

        rtree.UpdateTree();
        ltree.UpdateTree();
        rtree.collapseAboveLevel();
        ltree.collapseAboveLevel();
    }

    function createTree(words, treeOptions) {
        nodeID = -1;
        var tree = new WordTree(treeOptions);
        add(words, -1, tree, 0);
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
                paper       : paper,
                viewDocUrl  : viewDocUrl
            }
        };
    }

    function setJustDragged(paper) {
        WordTree.setJustDraggedStatus(paper, true);
    }

    function getAllElements(parentId) {
        var index = graphElToIdxMap[parentId];

        return WORDTREE.treeObj[index].ltree.paperSet.items.concat(WORDTREE.treeObj[index].rtree.paperSet.items,
               WORDTREE.treeObj[index].altQueryRaphaelElements);
    }

    var startAll = function () {
        var allElements = getAllElements(this.paper.canvas.parentNode.id);

        for(var i = 0; i < allElements.length; i++){
            var el = allElements[i], bbox = el.getBBox(), isPath = (allElements[i].type == "path");

            el.ox = isPath ? bbox.x : el.attr('x');
            el.oy = isPath ? bbox.y : el.attr('y');
        }
    };
    var moveAll = function (dx, dy) {
        var allElements = getAllElements(this.paper.canvas.parentNode.id);
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

    function getSelectedElementTransform(field) {
        var index = graphElToIdxMap[field],
            ltree = WORDTREE.treeObj[index].ltree,
            rtree = WORDTREE.treeObj[index].rtree,
            lsel = ltree.iSelectedNode,
            rsel = rtree.iSelectedNode;

        if (lsel == -1 && rsel == -1) return null;

        var selNode  = (lsel != -1) ? ltree.getNodeFromNodeId(lsel) : rtree.getNodeFromNodeId(rsel);
        var rootNode = (lsel != -1) ? ltree.getNodeFromNodeId(0)    : rtree.getNodeFromNodeId(0);

        return { dx : rootNode.XPosition - selNode.raphaelElements.boundingRect.attr('x'),
                 dy : rootNode.YPosition - selNode.raphaelElements.boundingRect.attr('y') };
    }

    WORDTREE.recenterOnSelected = function(field) {
        var allElements = getAllElements(field);
        var xform = getSelectedElementTransform(field);
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
        var paper      = conf['paper'],
            prefixes   = conf['prefixes'],
            suffixes   = conf['suffixes'],
            altQueries = conf['altQueries'];
        graphElHeight  = conf['graphElHeight'];
        viewDocUrl     = conf['viewDocUrl'];

        graphElToIdxMap[conf['graphElName']] = WORDTREE.treeObj.length;

        var rtree = createTree(suffixes, getTreeOptions(WordTree.RO_LEFT,  paper));
        var ltree = createTree(prefixes, getTreeOptions(WordTree.RO_RIGHT, paper));
        rtree.setOppositeTree(ltree);
        ltree.setOppositeTree(rtree);

        redraw(rtree, ltree);

        ltree.paperSet.drag(moveAll, startAll, upAll);
        rtree.paperSet.drag(moveAll, startAll, upAll);

        WORDTREE.treeObj.push({
            paper : paper,
            rtree : rtree,
            ltree : ltree,
            altQueryRaphaelElements : addAltQueries(paper, altQueries)
        });
    };

    WORDTREE.search = function(searchStr, searchFromSel, index) {
        var ltree = WORDTREE.treeObj[index].ltree,
            rtree = WORDTREE.treeObj[index].rtree;

        if (searchFromSel && ltree.iSelectedNode == -1 && rtree.iSelectedNode == -1) {
            searchFromSel = false;
        }

        return { rtreeNodes : (!searchFromSel || rtree.iSelectedNode != -1) ? rtree.search(searchStr, searchFromSel) : [],
                 ltreeNodes : (!searchFromSel || ltree.iSelectedNode != -1) ? ltree.search(searchStr, searchFromSel) : []};
    };
})();